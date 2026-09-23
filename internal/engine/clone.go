package engine

import (
	"fmt"
	"reflect"
	"time"
)

var timeType = reflect.TypeFor[time.Time]()

// Clone returns a deep copy of v, so a subscriber may mutate what it receives
// without reaching the engine's cache. A value reflection cannot copy, such as
// a channel or a func, is returned unchanged.
//
// The type switch is the whole of the optimisation: Clone sits on the read
// path (one call per consumer read) and on the delivery path (one per
// subscriber per change), and every value arriving from a store is what
// json.Unmarshal into `any` produces — nil, bool, float64, string,
// map[string]any, []any and nothing else. The reflective walk below copies
// those correctly but generically, at several times the cost. A registered
// default is arbitrary Go, so it still falls through.
func Clone(v any) any {
	switch value := v.(type) {
	case nil, bool, string,
		float32, float64,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, uintptr:
		// Immutable: a copy would alias nothing the caller can reach. json
		// only ever produces bool, float64 and string here; the rest are the
		// scalars a registered default may hold.
		return v
	case map[string]any:
		return cloneJSONObject(value)
	case []any:
		return cloneJSONArray(value)
	default:
		cloned, ok := cloneReflectValue(reflect.ValueOf(v))
		if !ok {
			return v
		}

		return cloned.Interface()
	}
}

// cloneJSONObject deep-copies a decoded JSON object. A nil map is returned as
// a nil map, which is what the reflective walk does with one.
func cloneJSONObject(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}

	out := make(map[string]any, len(m))
	for key, value := range m {
		out[key] = Clone(value)
	}

	return out
}

// cloneJSONArray deep-copies a decoded JSON array. A nil slice is returned as
// a nil slice, which is what the reflective walk does with one.
func cloneJSONArray(s []any) []any {
	if s == nil {
		return nil
	}

	out := make([]any, len(s))
	for i, value := range s {
		out[i] = Clone(value)
	}

	return out
}

func cloneReflectValue(v reflect.Value) (reflect.Value, bool) {
	if !v.IsValid() {
		return reflect.Value{}, false
	}

	switch v.Kind() {
	case reflect.Interface:
		return cloneInterfaceValue(v)
	case reflect.Pointer:
		return clonePointerValue(v)
	case reflect.Slice:
		return cloneSliceValue(v)
	case reflect.Array:
		return cloneArrayValue(v)
	case reflect.Map:
		return cloneMapValue(v)
	case reflect.Struct:
		return cloneStructValue(v)
	default:
		return v, true
	}
}

func cloneInterfaceValue(v reflect.Value) (reflect.Value, bool) {
	if v.IsNil() {
		return v, true
	}

	cloned, ok := cloneReflectValue(v.Elem())
	if !ok || !cloned.Type().AssignableTo(v.Elem().Type()) {
		return v, true
	}

	out := reflect.New(v.Elem().Type()).Elem()
	out.Set(cloned)

	return out, true
}

func clonePointerValue(v reflect.Value) (reflect.Value, bool) {
	if v.IsNil() {
		return v, true
	}

	cloned, ok := cloneReflectValue(v.Elem())
	if !ok || !cloned.Type().AssignableTo(v.Elem().Type()) {
		return v, true
	}

	out := reflect.New(v.Elem().Type())
	out.Elem().Set(cloned)

	return out, true
}

func cloneSliceValue(v reflect.Value) (reflect.Value, bool) {
	if v.IsNil() {
		return v, true
	}

	out := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
	for i := range v.Len() {
		setClonedValue(out.Index(i), v.Index(i))
	}

	return out, true
}

func cloneArrayValue(v reflect.Value) (reflect.Value, bool) {
	out := reflect.New(v.Type()).Elem()
	for i := range v.Len() {
		setClonedValue(out.Index(i), v.Index(i))
	}

	return out, true
}

func cloneMapValue(v reflect.Value) (reflect.Value, bool) {
	if v.IsNil() {
		return v, true
	}

	out := reflect.MakeMapWithSize(v.Type(), v.Len())
	for _, key := range v.MapKeys() {
		value := v.MapIndex(key)

		cloned, ok := cloneReflectValue(value)
		if !ok || !cloned.Type().AssignableTo(v.Type().Elem()) {
			cloned = value
		}

		out.SetMapIndex(key, cloned)
	}

	return out, true
}

func cloneStructValue(v reflect.Value) (reflect.Value, bool) {
	out := reflect.New(v.Type()).Elem()
	out.Set(v)

	for i := range v.NumField() {
		dst := out.Field(i)
		if !dst.CanSet() {
			continue
		}

		setClonedValue(dst, v.Field(i))
	}

	return out, true
}

func setClonedValue(dst, src reflect.Value) {
	cloned, ok := cloneReflectValue(src)
	if !ok || !cloned.Type().AssignableTo(dst.Type()) {
		cloned = src
	}

	dst.Set(cloned)
}

// ValidateCloneSafe reports whether v can be deep-copied by Clone without the
// copy aliasing the original: it rejects an unexported mutable field, which
// Clone cannot reach, and a cyclic reference, which Clone cannot terminate on.
func ValidateCloneSafe(v any) error {
	return validateCloneSafeValue(reflect.ValueOf(v), make(map[visit]struct{}), "value")
}

type visit struct {
	typ reflect.Type
	ptr uintptr
}

func validateCloneSafeValue(v reflect.Value, seen map[visit]struct{}, path string) error {
	if !v.IsValid() {
		return nil
	}

	for v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil
		}

		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Pointer:
		return validateCloneSafePointer(v, seen, path)
	case reflect.Slice, reflect.Array:
		return validateCloneSafeIndexed(v, seen, path)
	case reflect.Map:
		return validateCloneSafeMap(v, seen, path)
	case reflect.Struct:
		return validateCloneSafeStruct(v, seen, path)
	default:
		return nil
	}
}

func validateCloneSafePointer(v reflect.Value, seen map[visit]struct{}, path string) error {
	if v.IsNil() {
		return nil
	}

	mark := visit{typ: v.Type(), ptr: v.Pointer()}
	if _, ok := seen[mark]; ok {
		return fmt.Errorf("%s contains cyclic reference", path)
	}

	seen[mark] = struct{}{}
	defer delete(seen, mark)

	return validateCloneSafeValue(v.Elem(), seen, path)
}

func validateCloneSafeIndexed(v reflect.Value, seen map[visit]struct{}, path string) error {
	if v.Kind() == reflect.Slice {
		if v.IsNil() {
			return nil
		}

		mark := visit{typ: v.Type(), ptr: v.Pointer()}
		if _, ok := seen[mark]; ok {
			return fmt.Errorf("%s contains cyclic reference", path)
		}

		seen[mark] = struct{}{}
		defer delete(seen, mark)
	}

	for i := range v.Len() {
		if err := validateCloneSafeValue(v.Index(i), seen, fmt.Sprintf("%s[%d]", path, i)); err != nil {
			return err
		}
	}

	return nil
}

func validateCloneSafeMap(v reflect.Value, seen map[visit]struct{}, path string) error {
	if v.IsNil() {
		return nil
	}

	mark := visit{typ: v.Type(), ptr: v.Pointer()}
	if _, ok := seen[mark]; ok {
		return fmt.Errorf("%s contains cyclic reference", path)
	}

	seen[mark] = struct{}{}
	defer delete(seen, mark)

	for _, key := range v.MapKeys() {
		if err := validateCloneSafeValue(v.MapIndex(key), seen, path+"[map-value]"); err != nil {
			return err
		}
	}

	return nil
}

func validateCloneSafeStruct(v reflect.Value, seen map[visit]struct{}, path string) error {
	if v.Type() == timeType {
		return nil
	}

	for i := range v.NumField() {
		fieldInfo := v.Type().Field(i)
		field := v.Field(i)
		fieldPath := path + "." + fieldInfo.Name

		if fieldInfo.PkgPath != "" && mutableCloneKind(field.Kind()) {
			return fmt.Errorf("%s has unexported mutable field", fieldPath)
		}

		if err := validateCloneSafeValue(field, seen, fieldPath); err != nil {
			return err
		}
	}

	return nil
}

func mutableCloneKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return true
	default:
		return false
	}
}
