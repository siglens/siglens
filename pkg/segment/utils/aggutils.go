/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"fmt"
	"math"
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
		return e1, nil
	}

	// Convert to float if needed
	if e1.Dtype == SS_DT_FLOAT && e2.Dtype != SS_DT_FLOAT {
		switch e2.Dtype {
		case SS_DT_UNSIGNED_NUM:
			e2 = CValueEnclosure{Dtype: SS_DT_FLOAT, CVal: float64(e2.CVal.(uint64))}
		case SS_DT_SIGNED_NUM:
			e2 = CValueEnclosure{Dtype: SS_DT_FLOAT, CVal: float64(e2.CVal.(int64))}
		}
	}

	if e2.Dtype == SS_DT_FLOAT && e1.Dtype != SS_DT_FLOAT {
		switch e1.Dtype {
		case SS_DT_UNSIGNED_NUM:
			e1 = CValueEnclosure{Dtype: SS_DT_FLOAT, CVal: float64(e1.CVal.(uint64))}
		case SS_DT_SIGNED_NUM:
			e1 = CValueEnclosure{Dtype: SS_DT_FLOAT, CVal: float64(e1.CVal.(int64))}
		}
	}

	// TODO: what if one is int64 and the other is uint64? Is there any way to avoid annoying conversions?

	switch e1.Dtype {
	case SS_DT_UNSIGNED_NUM:
		switch fun {
		case Sum:
			return CValueEnclosure{Dtype: e1.Dtype, CVal: e1.CVal.(uint64) + e2.CVal.(uint64)}, nil
		case Min:
			return CValueEnclosure{Dtype: e1.Dtype, CVal: MinUint64(e1.CVal.(uint64), e2.CVal.(uint64))}, nil
		case Max:
			return CValueEnclosure{Dtype: e1.Dtype, CVal: MaxUint64(e1.CVal.(uint64), e2.CVal.(uint64))}, nil
		case Count:
			return CValueEnclosure{Dtype: e1.Dtype, CVal: e1.CVal.(uint64) + e2.CVal.(uint64)}, nil
		}
	case SS_DT_SIGNED_NUM:
		switch fun {
		case Sum:
			return CValueEnclosure{Dtype: e1.Dtype, CVal: e1.CVal.(int64) + e2.CVal.(int64)}, nil
		case Min:
			return CValueEnclosure{Dtype: e1.Dtype, CVal: MinInt64(e1.CVal.(int64), e2.CVal.(int64))}, nil
		case Max:
			return CValueEnclosure{Dtype: e1.Dtype, CVal: MaxInt64(e1.CVal.(int64), e2.CVal.(int64))}, nil
		case Count:
			return CValueEnclosure{Dtype: e1.Dtype, CVal: e1.CVal.(int64) + e2.CVal.(int64)}, nil
		}
	case SS_DT_FLOAT:
		switch fun {
		case Sum:
			return CValueEnclosure{Dtype: e1.Dtype, CVal: e1.CVal.(float64) + e2.CVal.(float64)}, nil
		case Min:
			return CValueEnclosure{Dtype: e1.Dtype, CVal: math.Min(e1.CVal.(float64), e2.CVal.(float64))}, nil
		case Max:
			return CValueEnclosure{Dtype: e1.Dtype, CVal: math.Max(e1.CVal.(float64), e2.CVal.(float64))}, nil
		case Count:
			return CValueEnclosure{Dtype: e1.Dtype, CVal: e1.CVal.(float64) + e2.CVal.(float64)}, nil
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
				return CValueEnclosure{Dtype: e1.Dtype, CVal: set1}, nil
			}
			return e1, fmt.Errorf("Reduce: unsupported CVal Dtype: %v", e1.Dtype)
		}
	default:
		return e1, fmt.Errorf("Reduce: unsupported CVal Dtype: %v", e1.Dtype)
	}
	return e1, fmt.Errorf("Reduce: unsupported reduce function: %v", fun)
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
		}
	}

	// if e2 is float and self is not, then convert self
	if e2Dtype == SS_DT_FLOAT && self.Ntype != SS_DT_FLOAT {
		switch self.Ntype {
		case SS_DT_UNSIGNED_NUM, SS_DT_SIGNED_NUM:
			self.Ntype = SS_DT_FLOAT
			self.FloatVal = float64(self.IntgrVal)
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
			self.IntgrVal = MinInt64(self.IntgrVal, e2int64)
			return nil
		case Max:
			self.IntgrVal = MaxInt64(self.IntgrVal, e2int64)
			return nil
		case Count:
			self.IntgrVal = self.IntgrVal + e2int64
			return nil
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
		}
	default:
		return fmt.Errorf("Reduce: unsupported self CVal Dtype: %v", self.Ntype)
	}
	return fmt.Errorf("Reduce: unsupported reduce function: %v", fun)
}
