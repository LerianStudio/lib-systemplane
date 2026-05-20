package systemplane

import "reflect"

func cloneValue(v any) any {
	cloned, ok := cloneReflectValue(reflect.ValueOf(v))
	if !ok {
		return v
	}

	return cloned.Interface()
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
	case reflect.Map:
		return cloneMapValue(v)
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

func setClonedValue(dst, src reflect.Value) {
	cloned, ok := cloneReflectValue(src)
	if !ok || !cloned.Type().AssignableTo(dst.Type()) {
		cloned = src
	}

	dst.Set(cloned)
}
