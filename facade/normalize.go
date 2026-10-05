package facade

// FieldStatus 标记归一化字段的来源状态，缺失/无法映射不得用默认值掩盖。
type FieldStatus string

const (
	FieldPresent  FieldStatus = "present"  // 后端返回且成功映射
	FieldMissing  FieldStatus = "missing"  // 后端未返回该字段
	FieldUnmapped FieldStatus = "unmapped" // 后端有返回但无法映射到统一模型
)

// Field 是一个归一化后的字段。
type Field struct {
	Status FieldStatus
	Value  string // 仅当 Status == FieldPresent 时有效
}

// NormalizedResult 是对外统一结果。
type NormalizedResult struct {
	Backend BackendID
	Op      OpKind
	Fields  map[string]Field
}

// Get 返回字段值与是否真实存在（缺失/未映射返回 ok=false）。
func (n *NormalizedResult) Get(name string) (string, bool) {
	f, ok := n.Fields[name]
	if !ok || f.Status != FieldPresent {
		return "", false
	}
	return f.Value, true
}

// FieldMapping 描述某后端原始字段名到统一字段名的映射。
// 统一字段名若不在映射中，结果里标记为 FieldUnmapped。
type FieldMapping map[string]string // canonical -> raw

// Normalizer 把某后端的原始响应归一化，并对缺失/未映射字段打标记。
type Normalizer struct {
	backend BackendID
	// canonicalFields 是该操作对外承诺的统一字段集合。
	canonicalFields map[OpKind][]string
	mappings        map[OpKind]FieldMapping
	logger          Logger
}

func NewNormalizer(backend BackendID, canonical map[OpKind][]string, mappings map[OpKind]FieldMapping, logger Logger) *Normalizer {
	if logger == nil {
		logger = nopLogger{}
	}
	return &Normalizer{backend: backend, canonicalFields: canonical, mappings: mappings, logger: logger}
}

// Normalize 将后端原始响应映射为统一结果。
func (n *Normalizer) Normalize(op OpKind, raw RawResponse) *NormalizedResult {
	res := &NormalizedResult{Backend: n.backend, Op: op, Fields: map[string]Field{}}
	mapping := n.mappings[op]
	for _, name := range n.canonicalFields[op] {
		rawName, ok := mapping[name]
		if !ok {
			res.Fields[name] = Field{Status: FieldUnmapped}
			n.logger.Log(Event{Time: now(), Reason: ReasonFieldUnmapped, Backend: n.backend, Op: op, Detail: name})
			continue
		}
		v, ok := raw.Fields[rawName]
		if !ok {
			res.Fields[name] = Field{Status: FieldMissing}
			n.logger.Log(Event{Time: now(), Reason: ReasonFieldMissing, Backend: n.backend, Op: op, Detail: name})
			continue
		}
		res.Fields[name] = Field{Status: FieldPresent, Value: v}
	}
	return res
}
