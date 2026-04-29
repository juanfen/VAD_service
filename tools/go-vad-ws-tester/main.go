package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/godeps/opus"
	"github.com/gorilla/websocket"
)

const (
	transportWS   = "ws"
	transportHTTP = "http"
	audioAuto     = "auto"
	audioPCM      = "pcm_s16le"
	audioOpus     = "opus"
)

type wavFile struct {
	AudioFormat   uint16
	Channels      uint16
	SampleRate    uint32
	BitsPerSample uint16
	Data          []byte
}

type serverMessage struct {
	Type               string `json:"type"`
	Code               any    `json:"code"`
	Message            string `json:"message"`
	SessionID          string `json:"session_id"`
	AudioFormat        string `json:"audio_format"`
	WorkerIndex        int    `json:"worker_index"`
	Sequence           int    `json:"sequence"`
	HaveVoice          bool   `json:"have_voice"`
	ClientHaveVoice    bool   `json:"client_have_voice"`
	ClientVoiceStop    bool   `json:"client_voice_stop"`
	LastIsVoice        bool   `json:"last_is_voice"`
	ProcessedChunks    int    `json:"processed_chunks"`
	PendingBufferBytes int    `json:"pending_buffer_bytes"`
	Deleted            bool   `json:"deleted"`
}

type deleteResponse struct {
	Deleted   bool   `json:"deleted"`
	SessionID string `json:"session_id"`
}

type errorResponse struct {
	Detail any `json:"detail"`
}

type payloadSet struct {
	frames         [][]byte
	audioFormat    string
	frameDuration  time.Duration
	description    string
	firstFrameSize int
}

type streamConfig struct {
	transport    string
	targetURL    string
	audioFormat  string
	threshold    float64
	thresholdLow float64
	minSilenceMS int
	frameWindow  int
	pcmFrameMS   int
	opusFrameMS  int
	opusBitrate  int
	realtime     bool
	apiKey       string
	verbose      bool
	httpTimeout  time.Duration
}

type runtimeContext struct {
	httpClient *http.Client
}

type loadConfig struct {
	streamConfig
	concurrency   int
	iterations    int
	duration      time.Duration
	progressEvery time.Duration
	sessionPrefix string
}

type loadStats struct {
	startedAt       time.Time
	sessionsStarted atomic.Uint64
	sessionsOK      atomic.Uint64
	sessionsFailed  atomic.Uint64
	framesStarted   atomic.Uint64
	framesOK        atomic.Uint64
	framesFailed    atomic.Uint64
	latencyMu       sync.Mutex
	latencies       []int64
	errorMu         sync.Mutex
	errorCounts     map[string]uint64
}

type streamObserver struct {
	onFrameStart   func(index int, total int)
	onFrameSuccess func(index int, total int, latency time.Duration, result serverMessage)
	onFrameError   func(index int, total int, err error)
}

type loadSnapshot struct {
	Elapsed         time.Duration
	SessionsStarted uint64
	SessionsOK      uint64
	SessionsFailed  uint64
	FramesStarted   uint64
	FramesOK        uint64
	FramesFailed    uint64
}

type loadSummary struct {
	loadSnapshot
	AverageLatency time.Duration
	P50Latency     time.Duration
	P95Latency     time.Duration
	P99Latency     time.Duration
	MaxLatency     time.Duration
	FrameRate      float64
	SessionRate    float64
	ErrorCounts    map[string]uint64
}

func main() {
	var (
		mode            = flag.String("mode", "single", "Run mode: single or load")
		transport       = flag.String("transport", transportWS, "Transport: ws or http")
		targetURL       = flag.String("url", "ws://127.0.0.1:8766/ws/v1/vad", "Target URL; websocket endpoint for ws, base URL or infer URL for http")
		audioFormat     = flag.String("audio-format", audioAuto, "Audio format: auto, pcm_s16le, or opus")
		wavPath         = flag.String("wav", "", "Path to a 16kHz mono 16-bit PCM WAV file")
		sessionID       = flag.String("session", fmt.Sprintf("go-test-%d", time.Now().Unix()), "Session ID for single mode")
		sessionPrefix   = flag.String("session-prefix", "load-test", "Session ID prefix for load mode")
		threshold       = flag.Float64("threshold", 0.5, "High threshold for speech detection")
		thresholdLow    = flag.Float64("threshold-low", 0.3, "Low threshold for speech detection")
		minSilenceMS    = flag.Int("min-silence-ms", 200, "Minimum silence duration in milliseconds")
		frameWindow     = flag.Int("frame-window-threshold", 3, "Speech frame window threshold")
		pcmFrameMS      = flag.Int("frame-ms", 32, "PCM frame duration in milliseconds for pcm_s16le transport")
		opusFrameMS     = flag.Int("opus-frame-ms", 20, "Opus packet duration in milliseconds; allowed: 5, 10, 20, 40, 60")
		opusBitrate     = flag.Int("opus-bitrate", 16000, "Opus bitrate in bits per second; set 0 to keep library default")
		realtime        = flag.Bool("realtime", true, "Sleep between frames to simulate realtime streaming")
		apiKey          = flag.String("api-key", "", "Optional API key without Bearer prefix")
		verboseResponse = flag.Bool("verbose", false, "Print detailed per-frame output")
		httpTimeout     = flag.Duration("http-timeout", 15*time.Second, "HTTP request timeout for transport=http")
		concurrency     = flag.Int("concurrency", 10, "Concurrent sessions in load mode")
		iterations      = flag.Int("iterations", 1, "How many sessions each worker goroutine should run in load mode")
		duration        = flag.Duration("duration", 0, "How long load mode should run, for example 30s or 2m; overrides iterations when > 0")
		progressEvery   = flag.Duration("progress", 5*time.Second, "Progress print interval in load mode")
	)
	flag.Parse()

	if *wavPath == "" {
		log.Fatal("missing required flag: -wav")
	}

	normalizedTransport := strings.ToLower(strings.TrimSpace(*transport))
	if normalizedTransport != transportWS && normalizedTransport != transportHTTP {
		log.Fatalf("unsupported transport: %s", *transport)
	}

	resolvedAudioFormat, err := resolveAudioFormat(normalizedTransport, *audioFormat)
	if err != nil {
		log.Fatal(err)
	}

	wav, err := loadWAV(*wavPath)
	if err != nil {
		log.Fatalf("load wav failed: %v", err)
	}
	if err := validateWAV(wav); err != nil {
		log.Fatalf("invalid wav file: %v", err)
	}
	if len(wav.Data) == 0 {
		log.Fatal("wav data chunk is empty")
	}

	cfg := streamConfig{
		transport:    normalizedTransport,
		targetURL:    *targetURL,
		audioFormat:  resolvedAudioFormat,
		threshold:    *threshold,
		thresholdLow: *thresholdLow,
		minSilenceMS: *minSilenceMS,
		frameWindow:  *frameWindow,
		pcmFrameMS:   *pcmFrameMS,
		opusFrameMS:  *opusFrameMS,
		opusBitrate:  *opusBitrate,
		realtime:     *realtime,
		apiKey:       *apiKey,
		verbose:      *verboseResponse,
		httpTimeout:  *httpTimeout,
	}

	payloads, err := buildPayloadSet(wav, cfg)
	if err != nil {
		log.Fatal(err)
	}

	switch strings.ToLower(strings.TrimSpace(*mode)) {
	case "single":
		rt := newRuntimeContext(cfg, 16)
		if err := runSingle(cfg, rt, *wavPath, *sessionID, payloads); err != nil {
			log.Fatal(err)
		}
	case "load":
		if *concurrency <= 0 {
			log.Fatal("-concurrency must be greater than 0")
		}
		if *iterations <= 0 && *duration <= 0 {
			log.Fatal("set -iterations > 0 or -duration > 0 for load mode")
		}

		loadCfg := loadConfig{
			streamConfig:  cfg,
			concurrency:   *concurrency,
			iterations:    *iterations,
			duration:      *duration,
			progressEvery: *progressEvery,
			sessionPrefix: *sessionPrefix,
		}

		rt := newRuntimeContext(cfg, maxInt(64, *concurrency*4))
		if err := runLoad(loadCfg, rt, *wavPath, payloads); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unsupported mode: %s", *mode)
	}
}

func runSingle(
	cfg streamConfig,
	rt runtimeContext,
	wavPath string,
	sessionID string,
	payloads payloadSet,
) error {
	switch cfg.transport {
	case transportWS:
		return runSingleWS(cfg, wavPath, sessionID, payloads)
	case transportHTTP:
		return runSingleHTTP(cfg, rt.httpClient, wavPath, sessionID, payloads)
	default:
		return fmt.Errorf("unsupported transport: %s", cfg.transport)
	}
}

func runSingleWS(
	cfg streamConfig,
	wavPath string,
	sessionID string,
	payloads payloadSet,
) error {
	conn, wsURL, err := connectWebSocket(cfg, sessionID)
	if err != nil {
		return err
	}
	defer conn.Close()

	fmt.Printf("transport=ws connected to %s\n", wsURL)
	fmt.Printf("wav file: %s\n", mustAbs(wavPath))
	fmt.Printf(
		"session=%s payloads=%d first_payload_bytes=%d audio_format=%s payload_shape=%s\n",
		sessionID,
		len(payloads.frames),
		payloads.firstFrameSize,
		payloads.audioFormat,
		payloads.description,
	)

	if err := streamWSFrames(conn, payloads, cfg, singleStreamObserver(cfg.verbose)); err != nil {
		return err
	}

	deleted, err := deleteWSSession(conn)
	if err != nil {
		return err
	}
	fmt.Printf("session deleted: %t\n", deleted)
	return nil
}

func runSingleHTTP(
	cfg streamConfig,
	client *http.Client,
	wavPath string,
	sessionID string,
	payloads payloadSet,
) error {
	if client == nil {
		return errors.New("http client is not initialized")
	}

	inferURL, err := buildHTTPInferURL(cfg, sessionID)
	if err != nil {
		return err
	}

	fmt.Printf("transport=http target=%s\n", inferURL)
	fmt.Printf("wav file: %s\n", mustAbs(wavPath))
	fmt.Printf(
		"session=%s payloads=%d first_payload_bytes=%d audio_format=%s payload_shape=%s\n",
		sessionID,
		len(payloads.frames),
		payloads.firstFrameSize,
		payloads.audioFormat,
		payloads.description,
	)

	if err := streamHTTPFrames(client, cfg, sessionID, payloads, singleStreamObserver(cfg.verbose)); err != nil {
		return err
	}

	deleted, err := deleteHTTPSession(client, cfg, sessionID)
	if err != nil {
		return err
	}
	fmt.Printf("session deleted: %t\n", deleted)
	return nil
}

func runLoad(
	cfg loadConfig,
	rt runtimeContext,
	wavPath string,
	payloads payloadSet,
) error {
	stats := &loadStats{
		startedAt:   time.Now(),
		errorCounts: make(map[string]uint64),
	}

	fmt.Println("load test started")
	fmt.Printf("wav file: %s\n", mustAbs(wavPath))
	fmt.Printf(
		"transport=%s target=%s audio_format=%s payload_shape=%s concurrency=%d iterations=%d duration=%s realtime=%t\n",
		cfg.transport,
		cfg.targetURL,
		payloads.audioFormat,
		payloads.description,
		cfg.concurrency,
		cfg.iterations,
		cfg.duration,
		cfg.realtime,
	)

	done := make(chan struct{})
	if cfg.progressEvery > 0 {
		go printProgress(stats, cfg.progressEvery, done)
	}

	var wg sync.WaitGroup
	startedAt := time.Now()
	var deadline time.Time
	if cfg.duration > 0 {
		deadline = startedAt.Add(cfg.duration)
	}

	for workerID := 0; workerID < cfg.concurrency; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runLoadWorker(id, cfg, rt, payloads, deadline, stats)
		}(workerID)
	}

	wg.Wait()
	close(done)

	summary := stats.summary()
	printLoadSummary(summary)

	if summary.FramesOK == 0 {
		return errors.New("load test finished without any successful frame response")
	}
	return nil
}

func runLoadWorker(
	workerID int,
	cfg loadConfig,
	rt runtimeContext,
	payloads payloadSet,
	deadline time.Time,
	stats *loadStats,
) {
	iteration := 0
	for {
		if !deadline.IsZero() && time.Now().After(deadline) {
			return
		}
		if deadline.IsZero() && iteration >= cfg.iterations {
			return
		}

		iteration++
		sessionID := fmt.Sprintf("%s-%03d-%06d", cfg.sessionPrefix, workerID, iteration)
		if err := runLoadSession(cfg.streamConfig, rt, sessionID, payloads, stats); err != nil {
			stats.recordSessionFailure(err)
			if cfg.verbose {
				log.Printf("session %s failed: %v", sessionID, err)
			}
		}
	}
}

func runLoadSession(
	cfg streamConfig,
	rt runtimeContext,
	sessionID string,
	payloads payloadSet,
	stats *loadStats,
) error {
	stats.sessionsStarted.Add(1)

	switch cfg.transport {
	case transportWS:
		conn, _, err := connectWebSocket(cfg, sessionID)
		if err != nil {
			return fmt.Errorf("connect failed: %w", err)
		}
		defer conn.Close()

		if err := streamWSFrames(conn, payloads, cfg, loadStreamObserver(stats)); err != nil {
			return err
		}

		deleted, err := deleteWSSession(conn)
		if err != nil {
			return err
		}
		if !deleted {
			return errors.New("server returned deleted=false")
		}
	case transportHTTP:
		if rt.httpClient == nil {
			return errors.New("http client is not initialized")
		}

		if err := streamHTTPFrames(rt.httpClient, cfg, sessionID, payloads, loadStreamObserver(stats)); err != nil {
			return err
		}

		deleted, err := deleteHTTPSession(rt.httpClient, cfg, sessionID)
		if err != nil {
			return err
		}
		if !deleted {
			return errors.New("server returned deleted=false")
		}
	default:
		return fmt.Errorf("unsupported transport: %s", cfg.transport)
	}

	stats.sessionsOK.Add(1)
	return nil
}

func newRuntimeContext(cfg streamConfig, maxIdleConns int) runtimeContext {
	if cfg.transport != transportHTTP {
		return runtimeContext{}
	}

	transport := &http.Transport{
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: maxIdleConns,
		IdleConnTimeout:     90 * time.Second,
	}

	return runtimeContext{
		httpClient: &http.Client{
			Timeout:   cfg.httpTimeout,
			Transport: transport,
		},
	}
}

func resolveAudioFormat(transport string, value string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if transport == transportWS {
		switch normalized {
		case "", audioAuto, audioOpus:
			return audioOpus, nil
		default:
			return "", fmt.Errorf("transport=ws only supports opus to preserve the original HTTP payload mode")
		}
	}

	switch normalized {
	case "", audioAuto:
		return audioOpus, nil
	case audioPCM, "pcm":
		return audioPCM, nil
	case audioOpus:
		return audioOpus, nil
	default:
		return "", fmt.Errorf("unsupported audio format: %s", value)
	}
}

func buildPayloadSet(wav *wavFile, cfg streamConfig) (payloadSet, error) {
	switch cfg.audioFormat {
	case audioPCM:
		return buildPCMPayloadSet(wav, cfg.pcmFrameMS)
	case audioOpus:
		return buildOpusPayloadSet(wav, cfg.opusFrameMS, cfg.opusBitrate)
	default:
		return payloadSet{}, fmt.Errorf("unsupported audio format: %s", cfg.audioFormat)
	}
}

func buildPCMPayloadSet(wav *wavFile, frameMS int) (payloadSet, error) {
	frameSize, err := calculatePCMFrameSize(wav, frameMS)
	if err != nil {
		return payloadSet{}, err
	}

	frames := buildFrames(wav.Data, frameSize)
	if len(frames) == 0 {
		return payloadSet{}, errors.New("pcm payload list is empty")
	}

	return payloadSet{
		frames:         frames,
		audioFormat:    audioPCM,
		frameDuration:  time.Duration(frameMS) * time.Millisecond,
		description:    fmt.Sprintf("pcm_frame_ms=%d frame_bytes=%d", frameMS, frameSize),
		firstFrameSize: len(frames[0]),
	}, nil
}

func buildOpusPayloadSet(wav *wavFile, frameMS int, bitrate int) (payloadSet, error) {
	if err := validateOpusFrameMS(frameMS); err != nil {
		return payloadSet{}, err
	}

	samples, err := pcmBytesToInt16(wav.Data)
	if err != nil {
		return payloadSet{}, err
	}

	channels := int(wav.Channels)
	samplesPerChannel, err := samplesPerFrame(int(wav.SampleRate), frameMS)
	if err != nil {
		return payloadSet{}, err
	}
	frameSamples := samplesPerChannel * channels

	encoder, err := opus.NewEncoder(int(wav.SampleRate), channels, opus.AppVoIP)
	if err != nil {
		return payloadSet{}, fmt.Errorf("create opus encoder failed: %w", err)
	}
	if bitrate > 0 {
		if err := encoder.SetBitrate(bitrate); err != nil {
			return payloadSet{}, fmt.Errorf("set opus bitrate failed: %w", err)
		}
	}

	packetBuffer := make([]byte, 4000)
	frames := make([][]byte, 0, (len(samples)+frameSamples-1)/frameSamples)

	for offset := 0; offset < len(samples); offset += frameSamples {
		pcmFrame := make([]int16, frameSamples)
		end := offset + frameSamples
		if end > len(samples) {
			end = len(samples)
		}
		copy(pcmFrame, samples[offset:end])

		n, err := encoder.Encode(pcmFrame, packetBuffer)
		if err != nil {
			return payloadSet{}, fmt.Errorf("encode opus packet failed: %w", err)
		}
		if n <= 0 {
			return payloadSet{}, errors.New("encode opus packet returned empty packet")
		}

		packet := append([]byte(nil), packetBuffer[:n]...)
		frames = append(frames, packet)
	}

	if len(frames) == 0 {
		return payloadSet{}, errors.New("opus payload list is empty")
	}

	description := fmt.Sprintf("opus_frame_ms=%d bitrate=%d", frameMS, bitrate)
	if bitrate <= 0 {
		description = fmt.Sprintf("opus_frame_ms=%d bitrate=encoder_default", frameMS)
	}

	return payloadSet{
		frames:         frames,
		audioFormat:    audioOpus,
		frameDuration:  time.Duration(frameMS) * time.Millisecond,
		description:    description,
		firstFrameSize: len(frames[0]),
	}, nil
}

func connectWebSocket(cfg streamConfig, sessionID string) (*websocket.Conn, string, error) {
	wsURL, err := buildWebSocketURL(cfg, sessionID)
	if err != nil {
		return nil, "", err
	}

	header := http.Header{}
	if strings.TrimSpace(cfg.apiKey) != "" {
		header.Set("Authorization", "Bearer "+strings.TrimSpace(cfg.apiKey))
	}

	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		if resp != nil {
			return nil, "", fmt.Errorf(
				"websocket connect failed: %w (http status: %s)",
				err,
				resp.Status,
			)
		}
		return nil, "", fmt.Errorf("websocket connect failed: %w", err)
	}

	return conn, wsURL, nil
}

func streamWSFrames(
	conn *websocket.Conn,
	payloads payloadSet,
	cfg streamConfig,
	observer streamObserver,
) error {
	for i, frame := range payloads.frames {
		if observer.onFrameStart != nil {
			observer.onFrameStart(i+1, len(payloads.frames))
		}

		startedAt := time.Now()
		if err := conn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
			if observer.onFrameError != nil {
				observer.onFrameError(i+1, len(payloads.frames), err)
			}
			return fmt.Errorf("send frame %d failed: %w", i+1, err)
		}

		var result serverMessage
		_, body, err := conn.ReadMessage()
		if err != nil {
			if observer.onFrameError != nil {
				observer.onFrameError(i+1, len(payloads.frames), err)
			}
			return fmt.Errorf("read frame %d response failed: %w", i+1, err)
		}

		result, err = decodeInferResponse(body)
		if err != nil {
			if observer.onFrameError != nil {
				observer.onFrameError(i+1, len(payloads.frames), err)
			}
			return fmt.Errorf("frame %d response error: %w", i+1, err)
		}

		latency := time.Since(startedAt)
		if observer.onFrameSuccess != nil {
			observer.onFrameSuccess(i+1, len(payloads.frames), latency, result)
		}

		if cfg.realtime && i < len(payloads.frames)-1 {
			time.Sleep(payloads.frameDuration)
		}
	}

	return nil
}

func deleteWSSession(conn *websocket.Conn) (bool, error) {
	if err := conn.WriteJSON(map[string]string{"type": "delete_session"}); err != nil {
		return false, fmt.Errorf("send delete_session command failed: %w", err)
	}

	_, body, err := conn.ReadMessage()
	if err != nil {
		return false, fmt.Errorf("read delete_session response failed: %w", err)
	}

	if deleteErr := decodeErrorBody(body); deleteErr != nil {
		return false, fmt.Errorf("delete_session returned error: %w", deleteErr)
	}

	var deleted deleteResponse
	if err := json.Unmarshal(body, &deleted); err != nil {
		return false, fmt.Errorf(
			"decode delete_session response failed: %w body=%s",
			err,
			trimBody(body),
		)
	}
	return deleted.Deleted, nil
}

func streamHTTPFrames(
	client *http.Client,
	cfg streamConfig,
	sessionID string,
	payloads payloadSet,
	observer streamObserver,
) error {
	inferURL, err := buildHTTPInferURL(cfg, sessionID)
	if err != nil {
		return err
	}

	for i, frame := range payloads.frames {
		if observer.onFrameStart != nil {
			observer.onFrameStart(i+1, len(payloads.frames))
		}

		startedAt := time.Now()
		result, err := doHTTPInfer(client, inferURL, cfg, frame)
		if err != nil {
			if observer.onFrameError != nil {
				observer.onFrameError(i+1, len(payloads.frames), err)
			}
			return fmt.Errorf("frame %d http infer failed: %w", i+1, err)
		}

		if observer.onFrameSuccess != nil {
			observer.onFrameSuccess(i+1, len(payloads.frames), time.Since(startedAt), result)
		}

		if cfg.realtime && i < len(payloads.frames)-1 {
			time.Sleep(payloads.frameDuration)
		}
	}

	return nil
}

func doHTTPInfer(
	client *http.Client,
	inferURL string,
	cfg streamConfig,
	payload []byte,
) (serverMessage, error) {
	req, err := http.NewRequest(http.MethodPost, inferURL, bytes.NewReader(payload))
	if err != nil {
		return serverMessage{}, fmt.Errorf("build http request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if strings.TrimSpace(cfg.apiKey) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(cfg.apiKey))
	}

	resp, err := client.Do(req)
	if err != nil {
		return serverMessage{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return serverMessage{}, fmt.Errorf("read http response failed: %w", err)
	}

	if resp.StatusCode >= 300 {
		return serverMessage{}, formatHTTPError(resp.StatusCode, body)
	}

	var result serverMessage
	if err := json.Unmarshal(body, &result); err != nil {
		return serverMessage{}, fmt.Errorf(
			"decode http response failed: %w body=%s",
			err,
			trimBody(body),
		)
	}
	return result, nil
}

func deleteHTTPSession(client *http.Client, cfg streamConfig, sessionID string) (bool, error) {
	deleteURL, err := buildHTTPDeleteURL(cfg, sessionID)
	if err != nil {
		return false, err
	}

	req, err := http.NewRequest(http.MethodDelete, deleteURL, nil)
	if err != nil {
		return false, fmt.Errorf("build delete request failed: %w", err)
	}
	if strings.TrimSpace(cfg.apiKey) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(cfg.apiKey))
	}

	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("read delete response failed: %w", err)
	}
	if resp.StatusCode >= 300 {
		return false, formatHTTPError(resp.StatusCode, body)
	}

	var deleted deleteResponse
	if err := json.Unmarshal(body, &deleted); err != nil {
		return false, fmt.Errorf(
			"decode delete response failed: %w body=%s",
			err,
			trimBody(body),
		)
	}
	return deleted.Deleted, nil
}

func formatHTTPError(statusCode int, body []byte) error {
	var payload errorResponse
	if err := json.Unmarshal(body, &payload); err == nil && payload.Detail != nil {
		return fmt.Errorf("code=%d message=%v", statusCode, payload.Detail)
	}
	return fmt.Errorf("code=%d body=%s", statusCode, trimBody(body))
}

func decodeInferResponse(body []byte) (serverMessage, error) {
	if err := decodeErrorBody(body); err != nil {
		return serverMessage{}, err
	}

	var result serverMessage
	if err := json.Unmarshal(body, &result); err != nil {
		return serverMessage{}, fmt.Errorf(
			"decode websocket response failed: %w body=%s",
			err,
			trimBody(body),
		)
	}
	return result, nil
}

func decodeErrorBody(body []byte) error {
	var payload errorResponse
	if err := json.Unmarshal(body, &payload); err == nil && payload.Detail != nil {
		return fmt.Errorf("%v", payload.Detail)
	}
	return nil
}

func buildWebSocketURL(cfg streamConfig, sessionID string) (string, error) {
	u, err := url.Parse(cfg.targetURL)
	if err != nil {
		return "", fmt.Errorf("invalid websocket url: %w", err)
	}

	query := u.Query()
	query.Set("session_id", sessionID)
	query.Set("audio_format", cfg.audioFormat)
	query.Set("threshold", fmt.Sprintf("%g", cfg.threshold))
	query.Set("threshold_low", fmt.Sprintf("%g", cfg.thresholdLow))
	query.Set("min_silence_duration_ms", fmt.Sprintf("%d", cfg.minSilenceMS))
	query.Set("frame_window_threshold", fmt.Sprintf("%d", cfg.frameWindow))
	u.RawQuery = query.Encode()
	return u.String(), nil
}

func buildHTTPInferURL(cfg streamConfig, sessionID string) (string, error) {
	base, prefix, err := parseHTTPBaseURL(cfg.targetURL)
	if err != nil {
		return "", err
	}

	base.Path = joinURLPath(prefix, "/v1/vad/infer")
	query := base.Query()
	query.Set("session_id", sessionID)
	query.Set("threshold", fmt.Sprintf("%g", cfg.threshold))
	query.Set("threshold_low", fmt.Sprintf("%g", cfg.thresholdLow))
	query.Set("min_silence_duration_ms", fmt.Sprintf("%d", cfg.minSilenceMS))
	query.Set("frame_window_threshold", fmt.Sprintf("%d", cfg.frameWindow))
	query.Set("audio_format", cfg.audioFormat)
	base.RawQuery = query.Encode()
	return base.String(), nil
}

func buildHTTPDeleteURL(cfg streamConfig, sessionID string) (string, error) {
	base, prefix, err := parseHTTPBaseURL(cfg.targetURL)
	if err != nil {
		return "", err
	}

	base.Path = joinURLPath(prefix, "/v1/vad/sessions/"+url.PathEscape(sessionID))
	base.RawQuery = ""
	return base.String(), nil
}

func parseHTTPBaseURL(raw string) (*url.URL, string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, "", fmt.Errorf("invalid http url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, "", fmt.Errorf("invalid http scheme: %s", u.Scheme)
	}

	prefix := strings.TrimRight(u.Path, "/")
	if strings.HasSuffix(prefix, "/v1/vad/infer") {
		prefix = strings.TrimSuffix(prefix, "/v1/vad/infer")
	}

	u.RawQuery = ""
	u.Fragment = ""
	return u, prefix, nil
}

func joinURLPath(prefix string, suffix string) string {
	left := strings.TrimRight(prefix, "/")
	right := strings.TrimLeft(suffix, "/")

	switch {
	case left == "" && right == "":
		return "/"
	case left == "":
		return "/" + right
	case right == "":
		return left
	default:
		return left + "/" + right
	}
}

func singleStreamObserver(verbose bool) streamObserver {
	return streamObserver{
		onFrameSuccess: func(index int, total int, latency time.Duration, result serverMessage) {
			if verbose {
				raw, _ := json.Marshal(result)
				fmt.Printf("frame %d/%d => %s latency=%s\n", index, total, string(raw), latency)
				return
			}

			fmt.Printf(
				"frame %d/%d => have_voice=%t client_have_voice=%t client_voice_stop=%t processed_chunks=%d pending_buffer_bytes=%d latency=%s\n",
				index,
				total,
				result.HaveVoice,
				result.ClientHaveVoice,
				result.ClientVoiceStop,
				result.ProcessedChunks,
				result.PendingBufferBytes,
				latency,
			)
		},
	}
}

func loadStreamObserver(stats *loadStats) streamObserver {
	return streamObserver{
		onFrameStart: func(index int, total int) {
			stats.recordFrameAttempt()
		},
		onFrameSuccess: func(index int, total int, latency time.Duration, result serverMessage) {
			stats.recordFrameOK(latency)
		},
		onFrameError: func(index int, total int, err error) {
			stats.recordFrameFailure(err)
		},
	}
}

func printProgress(stats *loadStats, every time.Duration, done <-chan struct{}) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			snapshot := stats.snapshot()
			fmt.Printf(
				"progress elapsed=%s sessions=%d ok=%d failed=%d frames=%d ok=%d failed=%d\n",
				snapshot.Elapsed.Truncate(time.Millisecond),
				snapshot.SessionsStarted,
				snapshot.SessionsOK,
				snapshot.SessionsFailed,
				snapshot.FramesStarted,
				snapshot.FramesOK,
				snapshot.FramesFailed,
			)
		}
	}
}

func (s *loadStats) snapshot() loadSnapshot {
	return loadSnapshot{
		Elapsed:         time.Since(s.startedAt),
		SessionsStarted: s.sessionsStarted.Load(),
		SessionsOK:      s.sessionsOK.Load(),
		SessionsFailed:  s.sessionsFailed.Load(),
		FramesStarted:   s.framesStarted.Load(),
		FramesOK:        s.framesOK.Load(),
		FramesFailed:    s.framesFailed.Load(),
	}
}

func (s *loadStats) recordFrameAttempt() {
	s.framesStarted.Add(1)
}

func (s *loadStats) recordFrameOK(latency time.Duration) {
	s.framesOK.Add(1)

	s.latencyMu.Lock()
	s.latencies = append(s.latencies, latency.Nanoseconds())
	s.latencyMu.Unlock()
}

func (s *loadStats) recordFrameFailure(err error) {
	s.framesFailed.Add(1)
	s.recordError(err)
}

func (s *loadStats) recordSessionFailure(err error) {
	s.sessionsFailed.Add(1)
	s.recordError(err)
}

func (s *loadStats) recordError(err error) {
	key := err.Error()
	if len(key) > 240 {
		key = key[:240]
	}

	s.errorMu.Lock()
	s.errorCounts[key]++
	s.errorMu.Unlock()
}

func (s *loadStats) summary() loadSummary {
	snapshot := s.snapshot()
	latencies := s.latencyCopy()

	summary := loadSummary{
		loadSnapshot: snapshot,
		ErrorCounts:  s.errorCopy(),
	}

	if len(latencies) == 0 {
		return summary
	}

	sort.Slice(latencies, func(i int, j int) bool {
		return latencies[i] < latencies[j]
	})

	var total int64
	for _, item := range latencies {
		total += item
	}

	summary.AverageLatency = time.Duration(total / int64(len(latencies)))
	summary.P50Latency = percentileDuration(latencies, 0.50)
	summary.P95Latency = percentileDuration(latencies, 0.95)
	summary.P99Latency = percentileDuration(latencies, 0.99)
	summary.MaxLatency = time.Duration(latencies[len(latencies)-1])

	elapsedSeconds := snapshot.Elapsed.Seconds()
	if elapsedSeconds > 0 {
		summary.FrameRate = float64(snapshot.FramesOK) / elapsedSeconds
		summary.SessionRate = float64(snapshot.SessionsOK) / elapsedSeconds
	}

	return summary
}

func (s *loadStats) latencyCopy() []int64 {
	s.latencyMu.Lock()
	defer s.latencyMu.Unlock()

	out := make([]int64, len(s.latencies))
	copy(out, s.latencies)
	return out
}

func (s *loadStats) errorCopy() map[string]uint64 {
	s.errorMu.Lock()
	defer s.errorMu.Unlock()

	out := make(map[string]uint64, len(s.errorCounts))
	for key, value := range s.errorCounts {
		out[key] = value
	}
	return out
}

func percentileDuration(sortedLatencies []int64, ratio float64) time.Duration {
	if len(sortedLatencies) == 0 {
		return 0
	}

	index := int(float64(len(sortedLatencies)-1) * ratio)
	if index < 0 {
		index = 0
	}
	if index >= len(sortedLatencies) {
		index = len(sortedLatencies) - 1
	}
	return time.Duration(sortedLatencies[index])
}

func printLoadSummary(summary loadSummary) {
	fmt.Println("load test finished")
	fmt.Printf("elapsed=%s\n", summary.Elapsed.Truncate(time.Millisecond))
	fmt.Printf(
		"sessions started=%d ok=%d failed=%d\n",
		summary.SessionsStarted,
		summary.SessionsOK,
		summary.SessionsFailed,
	)
	fmt.Printf(
		"frames started=%d ok=%d failed=%d\n",
		summary.FramesStarted,
		summary.FramesOK,
		summary.FramesFailed,
	)
	fmt.Printf(
		"throughput sessions_per_sec=%.2f frames_per_sec=%.2f\n",
		summary.SessionRate,
		summary.FrameRate,
	)

	if summary.FramesOK > 0 {
		fmt.Printf(
			"latency avg=%s p50=%s p95=%s p99=%s max=%s\n",
			summary.AverageLatency,
			summary.P50Latency,
			summary.P95Latency,
			summary.P99Latency,
			summary.MaxLatency,
		)
	}

	if len(summary.ErrorCounts) == 0 {
		return
	}

	type kv struct {
		Key   string
		Value uint64
	}
	items := make([]kv, 0, len(summary.ErrorCounts))
	for key, value := range summary.ErrorCounts {
		items = append(items, kv{Key: key, Value: value})
	}

	sort.Slice(items, func(i int, j int) bool {
		if items[i].Value == items[j].Value {
			return items[i].Key < items[j].Key
		}
		return items[i].Value > items[j].Value
	})

	fmt.Println("top errors:")
	limit := 5
	if len(items) < limit {
		limit = len(items)
	}
	for i := 0; i < limit; i++ {
		fmt.Printf("  %d x %s\n", items[i].Value, items[i].Key)
	}
}

func validateWAV(wav *wavFile) error {
	if wav.AudioFormat != 1 {
		return fmt.Errorf("unsupported wav format: got %d, want PCM (1)", wav.AudioFormat)
	}
	if wav.Channels != 1 {
		return fmt.Errorf("unsupported channel count: got %d, want 1", wav.Channels)
	}
	if wav.SampleRate != 16000 {
		return fmt.Errorf("unsupported sample rate: got %d, want 16000", wav.SampleRate)
	}
	if wav.BitsPerSample != 16 {
		return fmt.Errorf("unsupported bit depth: got %d, want 16", wav.BitsPerSample)
	}
	return nil
}

func calculatePCMFrameSize(wav *wavFile, frameMS int) (int, error) {
	bytesPerSample := int(wav.BitsPerSample / 8)
	samplesPerChannel, err := samplesPerFrame(int(wav.SampleRate), frameMS)
	if err != nil {
		return 0, err
	}
	return samplesPerChannel * int(wav.Channels) * bytesPerSample, nil
}

func validateOpusFrameMS(frameMS int) error {
	switch frameMS {
	case 5, 10, 20, 40, 60:
		return nil
	default:
		return fmt.Errorf("unsupported opus frame duration: %dms; allowed: 5, 10, 20, 40, 60", frameMS)
	}
}

func samplesPerFrame(sampleRate int, frameMS int) (int, error) {
	if frameMS <= 0 {
		return 0, errors.New("frame duration must be greater than 0")
	}

	samples := sampleRate * frameMS / 1000
	if samples <= 0 {
		return 0, errors.New("calculated frame sample count is 0")
	}
	if (sampleRate*frameMS)%1000 != 0 {
		return 0, fmt.Errorf(
			"frame duration %dms does not align with sample rate %d",
			frameMS,
			sampleRate,
		)
	}
	return samples, nil
}

func pcmBytesToInt16(data []byte) ([]int16, error) {
	if len(data)%2 != 0 {
		return nil, errors.New("pcm data length must be even")
	}

	samples := make([]int16, len(data)/2)
	for i := 0; i < len(samples); i++ {
		samples[i] = int16(binary.LittleEndian.Uint16(data[i*2 : i*2+2]))
	}
	return samples, nil
}

func buildFrames(data []byte, frameSize int) [][]byte {
	frames := make([][]byte, 0, (len(data)+frameSize-1)/frameSize)
	for start := 0; start < len(data); start += frameSize {
		end := start + frameSize
		if end > len(data) {
			end = len(data)
		}
		frames = append(frames, data[start:end])
	}
	return frames
}

func loadWAV(path string) (*wavFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	header := make([]byte, 12)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, err
	}
	if string(header[0:4]) != "RIFF" || string(header[8:12]) != "WAVE" {
		return nil, errors.New("file is not a RIFF/WAVE file")
	}

	result := &wavFile{}
	foundFmt := false
	foundData := false

	for {
		chunkHeader := make([]byte, 8)
		_, err := io.ReadFull(file, chunkHeader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		chunkID := string(chunkHeader[0:4])
		chunkSize := binary.LittleEndian.Uint32(chunkHeader[4:8])
		chunkData := make([]byte, chunkSize)
		if _, err := io.ReadFull(file, chunkData); err != nil {
			return nil, err
		}
		if chunkSize%2 == 1 {
			if _, err := file.Seek(1, io.SeekCurrent); err != nil {
				return nil, err
			}
		}

		switch chunkID {
		case "fmt ":
			if len(chunkData) < 16 {
				return nil, errors.New("wav fmt chunk is too short")
			}
			result.AudioFormat = binary.LittleEndian.Uint16(chunkData[0:2])
			result.Channels = binary.LittleEndian.Uint16(chunkData[2:4])
			result.SampleRate = binary.LittleEndian.Uint32(chunkData[4:8])
			result.BitsPerSample = binary.LittleEndian.Uint16(chunkData[14:16])
			foundFmt = true
		case "data":
			result.Data = chunkData
			foundData = true
		}

		if foundFmt && foundData {
			return result, nil
		}
	}

	if !foundFmt {
		return nil, errors.New("wav fmt chunk not found")
	}
	if !foundData {
		return nil, errors.New("wav data chunk not found")
	}
	return result, nil
}

func trimBody(body []byte) string {
	text := strings.TrimSpace(string(body))
	if len(text) > 240 {
		return text[:240]
	}
	return text
}

func mustAbs(path string) string {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return absPath
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
