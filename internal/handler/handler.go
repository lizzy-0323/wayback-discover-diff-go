package handler

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"

	"wayback-discover-diff/pkg/worker"
)

type Handler struct {
	redisClient *redis.Client
	taskClient  *asynq.Client
}

func NewHandler(redisClient *redis.Client, taskClient *asynq.Client) *Handler {
	return &Handler{
		redisClient: redisClient,
		taskClient:  taskClient,
	}
}

// CalculateSimHash handles requests to start simhash calculation
func (h *Handler) CalculateSimHash(c *gin.Context) {
	url := c.Query("url")
	yearStr := c.Query("year")

	if url == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "URL is required",
		})
		return
	}

	year, err := strconv.Atoi(yearStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid year format",
		})
		return
	}

	// Check if there's already a task running for this URL and year
	taskKey := fmt.Sprintf("task:%s:%d", url, year)
	exists, err := h.redisClient.Exists(context.Background(), taskKey).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Internal server error",
		})
		return
	}

	if exists == 1 {
		// Get existing task ID
		taskID, err := h.redisClient.Get(context.Background(), taskKey).Result()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": "Internal server error",
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status": "PENDING",
			"job_id": taskID,
		})
		return
	}

	// Create new task
	taskID := uuid.New().String()
	task := asynq.NewTask(worker.TypeCalculateSimHash, []byte(fmt.Sprintf(
		`{"url":"%s","year":%d}`, url, year)))

	_, err = h.taskClient.Enqueue(task, asynq.TaskID(taskID))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to create task",
		})
		return
	}

	// Store task information
	err = h.redisClient.Set(context.Background(), taskKey, taskID, 24*time.Hour).Err()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to store task information",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "started",
		"job_id": taskID,
	})
}

// GetSimHash handles requests to get simhash values
func (h *Handler) GetSimHash(c *gin.Context) {
	url := c.Query("url")
	timestamp := c.Query("timestamp")
	year := c.Query("year")
	compress := c.Query("compress")

	if url == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "URL is required",
		})
		return
	}

	// Handle single timestamp request
	if timestamp != "" {
		key := fmt.Sprintf("simhash:%s:%s", url, timestamp)
		simhash, err := h.redisClient.Get(context.Background(), key).Result()
		if err == redis.Nil {
			c.JSON(http.StatusNotFound, gin.H{
				"status":  "error",
				"message": "CAPTURE_NOT_FOUND",
			})
			return
		}
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": "Internal server error",
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"simhash": simhash,
		})
		return
	}

	// Handle year request
	if year != "" {
		pattern := fmt.Sprintf("simhash:%s:*", url)
		keys, err := h.redisClient.Keys(context.Background(), pattern).Result()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"status":  "error",
				"message": "Internal server error",
			})
			return
		}

		if len(keys) == 0 {
			c.JSON(http.StatusNotFound, gin.H{
				"status":  "error",
				"message": "NOT_CAPTURED",
			})
			return
		}

		// Get all simhash values
		captures := make([][]string, 0, len(keys))
		for _, key := range keys {
			simhash, err := h.redisClient.Get(context.Background(), key).Result()
			if err != nil {
				continue
			}
			timestamp := key[len(fmt.Sprintf("simhash:%s:", url)):]
			captures = append(captures, []string{timestamp, simhash})
		}

		// Check if task is still running
		taskKey := fmt.Sprintf("task:%s:%s", url, year)
		taskExists, _ := h.redisClient.Exists(context.Background(), taskKey).Result()
		status := "COMPLETE"
		if taskExists == 1 {
			status = "PENDING"
		}

		if compress == "1" {
			c.JSON(http.StatusOK, gin.H{
				"captures": captures,
				"total":    len(captures),
				"status":   status,
			})
		} else {
			c.JSON(http.StatusOK, captures)
		}
		return
	}

	c.JSON(http.StatusBadRequest, gin.H{
		"status":  "error",
		"message": "Either timestamp or year is required",
	})
}

// GetJobStatus handles requests to get job status
func (h *Handler) GetJobStatus(c *gin.Context) {
	jobID := c.Query("job_id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Job ID is required",
		})
		return
	}

	// Get task information from Redis
	inspector := asynq.NewInspector(asynq.RedisClientOpt{
		Addr: h.redisClient.Options().Addr,
	})

	taskInfo, err := inspector.GetTaskInfo("default", jobID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": "Job not found",
		})
		return
	}

	status := "pending"
	switch taskInfo.State {
	case asynq.TaskStateCompleted:
		status = "completed"
	case asynq.TaskStatePending:
		status = "pending"
	}

	c.JSON(http.StatusOK, gin.H{
		"status": status,
		"job_id": jobID,
	})
}
