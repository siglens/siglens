// Copyright (c) 2021-2024 SigScalr, Inc.
//
// This file is part of SigLens Observability Solution
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package utils

import (
	"fmt"
	"math"
	"strconv"
)

func Reduce(e1 CValueEnclosure, e2 CValueEnclosure, fun AggregateFunctions) (CValueEnclosure, error) {

	if e1.Dtype == SS_INVALID {
		return e2, nil
	} else if e2.Dtype == SS_INVALID {
		return e1, nil
	} else if e2.Dtype == SS_DT_BACKFILL {
		return e1, nil
	} else if e1.Dtype == SS_DT_BACKFILL {
		return e2, nil
	}

	// cannot reduce with incoming as string
	if e2.Dtype == SS_DT_STRING {
		if fun == Min || fun == Max {
			return ReduceMinMax(e1, e2, fun == Min)
		}
		return e1, nil
	}

	// Convert to float if needed
	if e1.Dtype == SS_DT_FLOAT && e2.Dtype != SS_DT_FLOAT {
		switch e2.Dtype {
		case SS_DT_UNSIGNED_NUM:
			e2.Dtype = SS_DT_FLOAT
			e2.CVal = float64(e2.CVal.(uint64))
		case SS_DT_SIGNED_NUM:
			e2.Dtype = SS_DT_FLOAT
			e2.CVal = float64(e2.CVal.(int64))
		default:
			return e1, fmt.Errorf("Reduce: unsupported e2 Dtype: %v", e2.Dtype)
		}
	}

	if e2.Dtype == SS_DT_FLOAT && e1.Dtype != SS_DT_FLOAT {
		switch e1.Dtype {
		case SS_DT_UNSIGNED_NUM:
			e1.Dtype = SS_DT_FLOAT
			e1.CVal = float64(e1.CVal.(uint64))
		case SS_DT_SIGNED_NUM:
			e1.Dtype = SS_DT_FLOAT
			e1.CVal = float64(e1.CVal.(int64))
		default:
			return e1, fmt.Errorf("Reduce: unsupported e1 Dtype: %v", e1.Dtype)
		}
	}

	// TODO: what if one is int64 and the other is uint64? Is there any way to avoid annoying conversions?

	switch e1.Dtype {
	case SS_DT_UNSIGNED_NUM:
		switch fun {
		case Sum, Count:
			e1.CVal = e1.CVal.(uint64) + e2.CVal.(uint64)
			return e1, nil
		case Min:
			e1.CVal = min(e1.CVal.(uint64), e2.CVal.(uint64))
			return e1, nil
		case Max:
			e1.CVal = max(e1.CVal.(uint64), e2.CVal.(uint64))
			return e1, nil
		default:
			return e1, fmt.Errorf("Reduce: unsupported aggregation type %v for unsigned int", fun)
		}
	case SS_DT_SIGNED_NUM:
		switch fun {
		case Sum, Count:
			e1.CVal = e1.CVal.(int64) + e2.CVal.(int64)
			return e1, nil
		case Min:
			e1.CVal = min(e1.CVal.(int64), e2.CVal.(int64))
			return e1, nil
		case Max:
			e1.CVal = max(e1.CVal.(int64), e2.CVal.(int64))
			return e1, nil
		default:
			return e1, fmt.Errorf("Reduce: unsupported aggregation type %v for signed int", fun)
		}
	case SS_DT_FLOAT:
		switch fun {
		case Sum, Count:
			e1.CVal = e1.CVal.(float64) + e2.CVal.(float64)
			return e1, nil
		case Min:
			e1.CVal = math.Min(e1.CVal.(float64), e2.CVal.(float64))
			return e1, nil
		case Max:
			e1.CVal = math.Max(e1.CVal.(float64), e2.CVal.(float64))
			return e1, nil
		default:
			return e1, fmt.Errorf("Reduce: unsupported aggregation type %v for float", fun)
		}
	case SS_DT_STRING_SET:
		{
			switch fun {
			case Cardinality:
				fallthrough
			case Values:
				set1 := e1.CVal.(map[string]struct{})
				set2 := e2.CVal.(map[string]struct{})
				for str := range set2 {
					set1[str] = struct{}{}
				}
				return e1, nil
			default:
				return e1, fmt.Errorf("Reduce: unsupported aggregation type %v for string set", fun)
			}
		}
	case SS_DT_STRING_SLICE:
		{
			if fun == List {
				list1 := e1.CVal.([]string)
				list2 := e2.CVal.([]string)
				list1 = append(list1, list2...)
				e1.CVal = list1
				return e1, nil
			} else {
				return e1, fmt.Errorf("Reduce: unsupported aggregation type %v for slice", fun)
			}
		}
	default:
		return e1, fmt.Errorf("Reduce: unsupported CVal Dtype: %v", e1.Dtype)
	}
}

func (self *NumTypeEnclosure) ReduceFast(e2Dtype SS_DTYPE, e2int64 int64,
	e2float64 float64, fun AggregateFunctions) error {

	if self.Ntype == SS_INVALID { // on first node we hit this, and we just use whatever is e2
		self.Ntype = e2Dtype
		switch e2Dtype {
		case SS_DT_UNSIGNED_NUM, SS_DT_SIGNED_NUM:
			self.IntgrVal = e2int64
		case SS_DT_FLOAT:
			self.FloatVal = e2float64
		default:
			return fmt.Errorf("ReduceFast: unsupported e2 Dtype: %v", e2Dtype)
		}
		return nil
	} else if e2Dtype == SS_INVALID { // if e2 is invalid, we live with whats in self
		return nil
	} else if e2Dtype == SS_DT_BACKFILL { // cant use e2 so return
		return nil
	} else if self.Ntype == SS_DT_BACKFILL { // if the first node happened to be backfill, then we use e2
		self.Ntype = e2Dtype
		switch e2Dtype {
		case SS_DT_UNSIGNED_NUM, SS_DT_SIGNED_NUM:
			self.IntgrVal = e2int64
		case SS_DT_FLOAT:
			self.FloatVal = e2float64
		default:
			return fmt.Errorf("ReduceFast: unsupported e2 Dtype: %v", e2Dtype)
		}
		return nil
	}

	// cannot reduce with incoming as string
	if e2Dtype == SS_DT_STRING {
		return nil
	}

	// if self is float and e2 is not, then convert e2
	if self.Ntype == SS_DT_FLOAT && e2Dtype != SS_DT_FLOAT {
		switch e2Dtype {
		case SS_DT_UNSIGNED_NUM, SS_DT_SIGNED_NUM:
			e2float64 = float64(e2int64)
		default:
			return fmt.Errorf("ReduceFast: unsupported e2 Dtype: %v", e2Dtype)
		}
	}

	// if e2 is float and self is not, then convert self
	if e2Dtype == SS_DT_FLOAT && self.Ntype != SS_DT_FLOAT {
		switch self.Ntype {
		case SS_DT_UNSIGNED_NUM, SS_DT_SIGNED_NUM:
			self.Ntype = SS_DT_FLOAT
			self.FloatVal = float64(self.IntgrVal)
		default:
			return fmt.Errorf("ReduceFast: unsupported self Dtype: %v", self.Ntype)
		}
	}

	// TODO: what if one is int64 and the other is uint64? Is there any way to avoid annoying conversions?
	// by now both sides are of same type
	switch self.Ntype {
	case SS_DT_SIGNED_NUM, SS_DT_UNSIGNED_NUM:
		switch fun {
		case Sum:
			self.IntgrVal = self.IntgrVal + e2int64
			return nil
		case Min:
			self.IntgrVal = min(self.IntgrVal, e2int64)
			return nil
		case Max:
			self.IntgrVal = max(self.IntgrVal, e2int64)
			return nil
		case Count:
			self.IntgrVal = self.IntgrVal + e2int64
			return nil
		default:
			return fmt.Errorf("ReduceFast: unsupported int function: %v", fun)
		}
	case SS_DT_FLOAT:
		switch fun {
		case Sum:
			self.FloatVal = self.FloatVal + e2float64
			return nil
		case Min:
			self.FloatVal = math.Min(self.FloatVal, e2float64)
			return nil
		case Max:
			self.FloatVal = math.Max(self.FloatVal, e2float64)
			return nil
		case Count:
			self.FloatVal = self.FloatVal + e2float64
			return nil
		default:
			return fmt.Errorf("ReduceFast: unsupported float function: %v", fun)
		}
	default:
		return fmt.Errorf("Reduce: unsupported self CVal Dtype: %v", self.Ntype)
	}
}

func GetMinMaxString(str1 string, str2 string, isMin bool) string {
	if isMin {
		if str1 < str2 {
			return str1
		}
		return str2
	} else {
		if str1 > str2 {
			return str1
		}
		return str2
	}
}

func ReduceMinMax(e1 CValueEnclosure, e2 CValueEnclosure, isMin bool) (CValueEnclosure, error) {
	if e1.Dtype == SS_INVALID || e1.Dtype == SS_DT_BACKFILL {
		return e2, nil
	}
	if e2.Dtype == SS_INVALID || e2.Dtype == SS_DT_BACKFILL {
		return e1, nil
	}

	if e1.Dtype == e2.Dtype {
		if e1.Dtype == SS_DT_STRING {
			return CValueEnclosure{Dtype: e1.Dtype, CVal: GetMinMaxString(e1.CVal.(string), e2.CVal.(string), isMin)}, nil
		} else {
			if isMin {
				return Reduce(e1, e2, Min)
			} else {
				return Reduce(e1, e2, Max)
			}
		}
	} else {
		if e1.IsNumeric() && e2.IsNumeric() {
			if isMin {
				return Reduce(e1, e2, Min)
			} else {
				return Reduce(e1, e2, Max)
			}
		} else if e1.IsNumeric() {
			return e1, nil
		} else {
			return e2, nil
		}
	}
}

func AppendWithLimit(dest []string, src []string, limit int) []string {
	remainingCapacity := limit - len(dest)
	if remainingCapacity <= 0 {
		return dest
	}
	if len(src) > remainingCapacity {
		return append(dest, src[:remainingCapacity]...)
	}
	return append(dest, src...)
}

// ParseStringToFloat64 tries to parse a string to a float64
func ParseStringToFloat64(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}
