// Package backends 提供各后端平台的参考适配器。
// 每个适配器在内部吸收该平台的数据格式差异（凭据形式、字段命名、
// 缺失字段），对外输出 facade 的归一化模型。
package backends

import (
	"context"
	"fmt"
	"strconv"
	"time"

	facade "github.com/shikejing/platform-sdk-facade-go/facade"
)

// RawRequest 一次原始后端调用。
type RawRequest struct {
	Op     string
	Cred   facade.Credential
	Params map[string]any
}

// Transport 执行原始调用并返回后端原生字段（模拟各平台的 SDK/HTTP 层）。
type Transport func(ctx context.Context, req RawRequest) (map[string]any, error)

// str 提取字符串字段；缺失返回 FieldMissing，类型不符返回 FieldUnmapped。
func str(m map[string]any, key string) facade.Field[string] {
	v, ok := m[key]
	if !ok || v == nil {
		return facade.Missing[string]()
	}
	s, ok := v.(string)
	if !ok {
		return facade.Unmapped[string]()
	}
	return facade.OK(s)
}

// integer 提取整数字段（JSON 数字通常为 float64）。
func integer(m map[string]any, key string) facade.Field[int] {
	v, ok := m[key]
	if !ok || v == nil {
		return facade.Missing[int]()
	}
	switch n := v.(type) {
	case float64:
		return facade.OK(int(n))
	case int:
		return facade.OK(n)
	case string:
		if i, err := strconv.Atoi(n); err == nil {
			return facade.OK(i)
		}
		return facade.Unmapped[int]()
	default:
		return facade.Unmapped[int]()
	}
}

// integer64 提取 int64 字段。
func integer64(m map[string]any, key string) facade.Field[int64] {
	v, ok := m[key]
	if !ok || v == nil {
		return facade.Missing[int64]()
	}
	switch n := v.(type) {
	case float64:
		return facade.OK(int64(n))
	case int64:
		return facade.OK(n)
	case string:
		if i, err := strconv.ParseInt(n, 10, 64); err == nil {
			return facade.OK(i)
		}
		return facade.Unmapped[int64]()
	default:
		return facade.Unmapped[int64]()
	}
}

// durationMs 提取毫秒表示的时长字段。
func durationMs(m map[string]any, key string) facade.Field[time.Duration] {
	v, ok := m[key]
	if !ok || v == nil {
		return facade.Missing[time.Duration]()
	}
	switch n := v.(type) {
	case float64:
		return facade.OK(time.Duration(n) * time.Millisecond)
	case string:
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return facade.OK(time.Duration(f) * time.Millisecond)
		}
		return facade.Unmapped[time.Duration]()
	default:
		return facade.Unmapped[time.Duration]()
	}
}

// durationSec 提取秒表示的时长字段。
func durationSec(m map[string]any, key string) facade.Field[time.Duration] {
	v, ok := m[key]
	if !ok || v == nil {
		return facade.Missing[time.Duration]()
	}
	switch n := v.(type) {
	case float64:
		return facade.OK(time.Duration(n * float64(time.Second)))
	case string:
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return facade.OK(time.Duration(f * float64(time.Second)))
		}
		return facade.Unmapped[time.Duration]()
	default:
		return facade.Unmapped[time.Duration]()
	}
}

// list 提取对象数组字段。
func list(m map[string]any, key string) ([]map[string]any, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return nil, nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("field %q is not a list", key)
	}
	out := make([]map[string]any, 0, len(arr))
	for _, item := range arr {
		obj, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("field %q contains non-object item", key)
		}
		out = append(out, obj)
	}
	return out, nil
}
