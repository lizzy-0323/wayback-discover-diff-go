package simhash

import (
	"bytes"
	"encoding/base64"
	"strings"
	"unicode"

	"golang.org/x/crypto/blake2b"
	"golang.org/x/net/html"
)

// Feature represents a text feature with its weight
type Feature struct {
	Text   string
	Weight int
}

// ExtractHTMLFeatures processes HTML document and extracts key features
func ExtractHTMLFeatures(htmlContent []byte) map[string]int {
	features := make(map[string]int)

	doc, err := html.Parse(bytes.NewReader(htmlContent))
	if err != nil {
		return features
	}

	var text strings.Builder
	var extractText func(*html.Node)
	extractText = func(n *html.Node) {
		if n.Type == html.TextNode {
			text.WriteString(n.Data + " ")
		}
		if n.Type == html.ElementNode {
			// Skip script and style elements
			if n.Data == "script" || n.Data == "style" {
				return
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			extractText(c)
		}
	}

	extractText(doc)

	// Process the extracted text
	words := strings.Fields(strings.ToLower(text.String()))
	for _, word := range words {
		// Remove punctuation and non-letter characters
		word = strings.Map(func(r rune) rune {
			if unicode.IsPunct(r) || !unicode.IsLetter(r) {
				return ' '
			}
			return r
		}, word)

		word = strings.TrimSpace(word)
		if word != "" {
			features[word]++
		}
	}

	return features
}

// CalculateSimHash computes the simhash for the given features
func CalculateSimHash(features map[string]int, size int) uint64 {
	if size > 64 {
		size = 64 // Go uint64 limitation
	}

	vectors := make([]int, size)

	// For each feature
	for text, weight := range features {
		// Calculate hash using BLAKE2b
		hash := blake2b.Sum512([]byte(text))
		hashBits := toBits(hash[:], size)

		// Add/subtract the weight of the features
		for i := 0; i < size; i++ {
			if hashBits[i] {
				vectors[i] += weight
			} else {
				vectors[i] -= weight
			}
		}
	}

	// Build the final hash
	var simhash uint64
	for i := 0; i < size; i++ {
		if vectors[i] > 0 {
			simhash |= 1 << uint(i)
		}
	}

	return simhash
}

// toBits converts a byte slice to a boolean slice representing bits
func toBits(hash []byte, size int) []bool {
	bits := make([]bool, size)
	for i := 0; i < size && i < len(hash)*8; i++ {
		byteIndex := i / 8
		bitIndex := uint(7 - (i % 8))
		bits[i] = (hash[byteIndex]>>bitIndex)&1 == 1
	}
	return bits
}

// EncodeSimHash encodes a simhash value to base64
func EncodeSimHash(simhash uint64) string {
	bytes := make([]byte, 8)
	for i := 0; i < 8; i++ {
		bytes[i] = byte(simhash >> uint(i*8))
	}
	return base64.StdEncoding.EncodeToString(bytes)
}

// DecodeSimHash decodes a base64 encoded simhash value
func DecodeSimHash(encoded string) (uint64, error) {
	bytes, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return 0, err
	}

	var simhash uint64
	for i := 0; i < len(bytes) && i < 8; i++ {
		simhash |= uint64(bytes[i]) << uint(i*8)
	}
	return simhash, nil
}
