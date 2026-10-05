package facade

import (
	"testing"
)

func testNormalizer(logger Logger) *Normalizer {
	return NewNormalizer("b1",
		map[OpKind][]string{OpAccountGet: {"account_id", "name", "region"}},
		map[OpKind]FieldMapping{OpAccountGet: {
			"account_id": "uid",
			"name":       "nick",
			// region 故意不映射 -> FieldUnmapped
		}},
		logger)
}

func TestNormalizePresentMissingUnmapped(t *testing.T) {
	logger := NewMemLogger()
	n := testNormalizer(logger)

	res := n.Normalize(OpAccountGet, RawResponse{Fields: map[string]string{
		"uid": "u-1",
		// nick 缺失 -> FieldMissing
	}})

	if f := res.Fields["account_id"]; f.Status != FieldPresent || f.Value != "u-1" {
		t.Fatalf("account_id should be present, got %+v", f)
	}
	if f := res.Fields["name"]; f.Status != FieldMissing {
		t.Fatalf("name should be missing, got %+v", f)
	}
	if f := res.Fields["region"]; f.Status != FieldUnmapped {
		t.Fatalf("region should be unmapped, got %+v", f)
	}

	// Get 不得用默认值掩盖缺失/未映射。
	if v, ok := res.Get("name"); ok || v != "" {
		t.Fatal("Get must report missing field as not-ok")
	}
	if v, ok := res.Get("region"); ok || v != "" {
		t.Fatal("Get must report unmapped field as not-ok")
	}
	if v, ok := res.Get("account_id"); !ok || v != "u-1" {
		t.Fatal("Get must return present field")
	}

	// 审计事件可区分缺失与未映射。
	if len(logger.ByReason(ReasonFieldMissing)) != 1 {
		t.Fatal("missing FIELD_MISSING event")
	}
	if len(logger.ByReason(ReasonFieldUnmapped)) != 1 {
		t.Fatal("missing FIELD_UNMAPPED event")
	}
}
