package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq"

	"wayback-discover-diff/config"
	"wayback-discover-diff/pkg/simhash"
)

const (
	TypeCalculateSimHash = "simhash:calculate"
	maxDownloadSize      = 1000000 // 1MB
)

type Worker struct {
	redisClient  *redis.Client
	httpClient   *http.Client
	downloadErrs int
	mutex        sync.Mutex
}

type SimHashPayload struct {
	URL  string `json:"url"`
	Year int    `json:"year"`
}

func NewWorker(redisClient *redis.Client) *Worker {
	return &Worker{
		redisClient: redisClient,
		httpClient: &http.Client{
			Timeout: time.Second * 20,
		},
	}
}

func (w *Worker) HandleCalculateSimHash(ctx context.Context, t *asynq.Task) error {
	var p SimHashPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v", err)
	}

	// Reset error counter for new task
	w.mutex.Lock()
	w.downloadErrs = 0
	w.mutex.Unlock()

	// Process URL for the given year
	return w.processURLForYear(ctx, p.URL, p.Year)
}

func (w *Worker) processURLForYear(ctx context.Context, url string, year int) error {
	// Get snapshots for the year
	snapshots, err := w.getSnapshots(url, year)
	if err != nil {
		return err
	}

	// Process each snapshot
	for _, snap := range snapshots {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := w.processSnapshot(ctx, url, snap); err != nil {
				w.incrementErrors()
				if w.getErrorCount() >= config.AppConfig.MaxErrors {
					return fmt.Errorf("max errors reached: %d", config.AppConfig.MaxErrors)
				}
				continue
			}
		}
	}

	return nil
}

func (w *Worker) processSnapshot(ctx context.Context, url string, timestamp string) error {
	// Check if we already have this snapshot processed
	key := fmt.Sprintf("simhash:%s:%s", url, timestamp)
	exists, err := w.redisClient.Exists(ctx, key).Result()
	if err != nil {
		return err
	}
	if exists == 1 {
		return nil
	}

	// Download snapshot
	content, err := w.downloadSnapshot(url, timestamp)
	if err != nil {
		return err
	}

	// Extract features and calculate simhash
	features := simhash.ExtractHTMLFeatures(content)
	if len(features) == 0 {
		return fmt.Errorf("no features extracted")
	}

	hash := simhash.CalculateSimHash(features, config.AppConfig.Simhash.Size)
	encoded := simhash.EncodeSimHash(hash)

	// Store in Redis
	return w.redisClient.Set(ctx, key, encoded,
		time.Duration(config.AppConfig.Simhash.ExpireAfter)*time.Second).Err()
}

func (w *Worker) downloadSnapshot(url, timestamp string) ([]byte, error) {
	waybackURL := fmt.Sprintf("http://web.archive.org/web/%sid_/%s", timestamp, url)

	req, err := http.NewRequest("GET", waybackURL, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", "wayback-discover-diff")
	if config.AppConfig.CdxAuthToken != "" {
		req.Header.Set("Cookie", fmt.Sprintf("cdx_auth_token=%s", config.AppConfig.CdxAuthToken))
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if !isHTMLContent(contentType) {
		return nil, fmt.Errorf("not HTML content: %s", contentType)
	}

	return ioutil.ReadAll(resp.Body)
}

func (w *Worker) getSnapshots(url string, year int) ([]string, error) {
	cdxURL := fmt.Sprintf("http://web.archive.org/cdx/search/cdx?url=%s&from=%d&to=%d&output=json",
		url, year, year)

	resp, err := w.httpClient.Get(cdxURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var results [][]string
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, err
	}

	if len(results) < 2 {
		return nil, fmt.Errorf("no snapshots found")
	}

	// Extract timestamps (skip header row)
	timestamps := make([]string, 0, len(results)-1)
	for _, row := range results[1:] {
		if len(row) > 1 {
			timestamps = append(timestamps, row[1])
		}
	}

	return timestamps, nil
}

func (w *Worker) incrementErrors() {
	w.mutex.Lock()
	w.downloadErrs++
	w.mutex.Unlock()
}

func (w *Worker) getErrorCount() int {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.downloadErrs
}

func isHTMLContent(contentType string) bool {
	return strings.Contains(strings.ToLower(contentType), "text/html") ||
		strings.Contains(strings.ToLower(contentType), "application/xhtml")
}
