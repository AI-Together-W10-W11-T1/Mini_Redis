package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

type AOFInspection struct {
	Path       string `json:"path"`
	Bytes      int64  `json:"bytes"`
	ValidBytes int64  `json:"valid_bytes"`
	Records    int    `json:"records"`
	Problem    string `json:"problem,omitempty"`
	ZeroTail   bool   `json:"zero_tail"`
	Output     string `json:"output,omitempty"`
}

func recoverySource(path string) (*os.File, *os.File, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, nil, err
	}
	lock, err := os.OpenFile(abs+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, nil, err
	}
	if err = lockFile(lock); err != nil {
		lock.Close()
		return nil, nil, fmt.Errorf("stop the server before inspection/recovery: %w", err)
	}
	f, err := os.Open(abs)
	if err != nil {
		lock.Close()
		return nil, nil, err
	}
	return f, lock, nil
}

// InspectAOF never truncates the source. All-zero tails are reported, not
// silently assumed to be safe padding or permission to discard records.
func InspectAOF(path string) (AOFInspection, error) {
	f, lock, err := recoverySource(path)
	if err != nil {
		return AOFInspection{}, err
	}
	defer f.Close()
	defer lock.Close()
	return inspectAOF(f)
}

func inspectAOF(f *os.File) (AOFInspection, error) {
	info, err := f.Stat()
	if err != nil {
		return AOFInspection{}, err
	}
	report := AOFInspection{Path: f.Name(), Bytes: info.Size()}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return report, err
	}
	header := make([]byte, len(aofMagic))
	if _, err = io.ReadFull(f, header); err != nil || string(header) != aofMagic {
		report.Problem = "invalid or incomplete AOF header"
		return report, nil
	}
	report.ValidBytes = int64(len(aofMagic))
	for {
		var h [8]byte
		n, err := io.ReadFull(f, h[:])
		if err == io.EOF && n == 0 {
			return report, nil
		}
		if err != nil {
			report.Problem = "incomplete frame header"
			break
		}
		if crc32.ChecksumIEEE(h[:4]) != binary.BigEndian.Uint32(h[4:]) {
			report.Problem = "frame header checksum mismatch"
			break
		}
		length := binary.BigEndian.Uint32(h[:4])
		if length == 0 || length > maxFrame {
			report.Problem = "invalid frame length"
			break
		}
		payload := make([]byte, int(length)+4)
		if _, err = io.ReadFull(f, payload); err != nil {
			report.Problem = "incomplete frame body"
			break
		}
		if crc32.ChecksumIEEE(payload[:length]) != binary.BigEndian.Uint32(payload[length:]) {
			report.Problem = "body checksum mismatch"
			break
		}
		if _, err = decodeAOFRecord(payload[:length]); err != nil {
			report.Problem = err.Error()
			break
		}
		report.Records++
		report.ValidBytes += int64(length) + 12
	}
	if _, err = f.Seek(report.ValidBytes, io.SeekStart); err != nil {
		return report, err
	}
	buffer := make([]byte, 32*1024)
	report.ZeroTail = report.ValidBytes < report.Bytes
	for {
		n, err := f.Read(buffer)
		for _, b := range buffer[:n] {
			if b != 0 {
				report.ZeroTail = false
				return report, nil
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return report, err
		}
	}
	return report, nil
}

func decodeAOFRecord(payload []byte) (aofRecord, error) {
	var r aofRecord
	d := json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return r, err
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return r, fmt.Errorf("trailing record data")
	}
	if err := validateKey(r.Key); err != nil {
		return r, err
	}
	if len(r.Value) > MaxValueBytes {
		return r, fmt.Errorf("oversized record value")
	}
	switch r.Op {
	case "set":
	case "del":
		if r.ExpiresAt != nil || len(r.Value) != 0 {
			return r, fmt.Errorf("invalid delete record")
		}
	default:
		return r, fmt.Errorf("unknown record operation")
	}
	return r, nil
}

// RecoverAOF writes only the validated prefix to a NEW file. The source is
// preserved; dropping any suffix requires explicit acknowledgement of loss.
func RecoverAOF(input, output string, allowDataLoss bool) (AOFInspection, error) {
	f, lock, err := recoverySource(input)
	if err != nil {
		return AOFInspection{}, err
	}
	defer f.Close()
	defer lock.Close()
	report, err := inspectAOF(f)
	if err != nil {
		return report, err
	}
	if report.ValidBytes < int64(len(aofMagic)) {
		return report, fmt.Errorf("no valid AOF header to recover")
	}
	if report.Problem != "" && !allowDataLoss {
		return report, fmt.Errorf("recovery discards %d bytes; pass -allow-data-loss after reviewing inspection", report.Bytes-report.ValidBytes)
	}
	target, err := filepath.Abs(output)
	if err != nil {
		return report, err
	}
	if target == f.Name() {
		return report, fmt.Errorf("output must differ from input")
	}
	outputLock, err := os.OpenFile(target+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return report, err
	}
	defer outputLock.Close()
	if err = lockFile(outputLock); err != nil {
		return report, fmt.Errorf("output AOF is in use: %w", err)
	}
	if _, err = os.Lstat(target); err == nil {
		return report, fmt.Errorf("output already exists")
	}
	if !os.IsNotExist(err) {
		return report, err
	}
	dest, err := os.CreateTemp(filepath.Dir(target), ".aof-recovery-*")
	if err != nil {
		return report, err
	}
	defer os.Remove(dest.Name())
	complete := false
	published := false
	defer func() {
		dest.Close()
		if published && !complete {
			os.Remove(target)
		}
	}()
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return report, err
	}
	if _, err = io.CopyN(dest, f, report.ValidBytes); err != nil {
		return report, err
	}
	if err = dest.Sync(); err != nil {
		return report, err
	}
	if err = dest.Close(); err != nil {
		return report, err
	}
	// 같은 디렉터리의 완성된 파일만 기존 경로를 덮어쓰지 않고 공개한다.
	if err = os.Link(dest.Name(), target); err != nil {
		return report, err
	}
	published = true
	if err = syncDirectory(filepath.Dir(target)); err != nil {
		return report, err
	}
	complete = true
	report.Output = target
	return report, nil
}
