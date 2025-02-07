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
			e1.CVal = MinUint64(e1.CVal.(uint64), e2.CVal.(uint64))
			return e1, nil
		case Max:
			e1.CVal = MaxUint64(e1.CVal.(uint64), e2.CVal.(uint64))
			return e1, nil
		case Sumsq:
			val2 := e2.CVal.(uint64)

			// Prevent overflow while computing sum of squares
			if val2 > math.MaxUint64/val2 {
				return e1, fmt.Errorf("Reduce: uint64 overflow detected in sumsq operation")
			}

			squaredVal2 := val2 * val2

			if e1.CVal.(uint64) > math.MaxUint64-squaredVal2 {
				return e1, fmt.Errorf("Reduce: uint64 addition overflow in sumsq operation")
			}

			e1.CVal = e1.CVal.(uint64) + squaredVal2
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
			e1.CVal = MinInt64(e1.CVal.(int64), e2.CVal.(int64))
			return e1, nil
		case Max:
			e1.CVal = MaxInt64(e1.CVal.(int64), e2.CVal.(int64))
			return e1, nil
		case Sumsq:

			val2 := e2.CVal.(int64)

			// Convert to uint64 to handle large negative values safely
			absVal2 := uint64(math.Abs(float64(val2)))

			// Prevent overflow while computing sum of squares
			if absVal2 > math.MaxUint64/absVal2 {
				return e1, fmt.Errorf("Reduce: int64 overflow detected in sumsq operation")
			}

			squaredVal2 := int64(absVal2 * absVal2)

			// Handle first assignment case
			if e1.CVal.(int64) == 0 {
				e1.CVal = squaredVal2
				return e1, nil
			}

			// Prevent overflow while adding squared values
			if e1.CVal.(int64) > math.MaxInt64-squaredVal2 {
				return e1, fmt.Errorf("Reduce: int64 addition overflow in sumsq operation")
			}

			e1.CVal = e1.CVal.(int64) + squaredVal2
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
		case Sumsq:
			val2 := e2.CVal.(float64)

			// Handle NaN cases to prevent propagation
			if math.IsNaN(val2) || math.IsNaN(e1.CVal.(float64)) {
				return e1, fmt.Errorf("Reduce: NaN detected in sumsq operation")
			}

			// Check for Infinity to prevent unexpected behavior
			if math.IsInf(val2, 0) || math.IsInf(e1.CVal.(float64), 0) {
				return e1, fmt.Errorf("Reduce: Infinity detected in sumsq operation")
			}

			squaredVal2 := val2 * val2

			// Handle first assignment case
			if e1.CVal.(float64) == 0 {
				e1.CVal = squaredVal2
				return e1, nil
			}

			// Prevent overflow when adding squared values
			if math.IsInf(e1.CVal.(float64)+squaredVal2, 0) {
				return e1, fmt.Errorf("Reduce: float64 overflow detected in sumsq operation")
			}

			e1.CVal = e1.CVal.(float64) + squaredVal2
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
			self.IntgrVal = MinInt64(self.IntgrVal, e2int64)
			return nil
		case Max:
			self.IntgrVal = MaxInt64(self.IntgrVal, e2int64)
			return nil
		case Count:
			self.IntgrVal = self.IntgrVal + e2int64
			return nil
		case Sumsq:
			// Handle first-time initialization
			if self.Ntype == SS_INVALID {
				self.Ntype = SS_DT_UNSIGNED_NUM
				squared := uint64(e2int64) * uint64(e2int64)
				self.IntgrVal = int64(squared) // Store as int64 while keeping positive range
				return nil
			}

			// Handle signed integers safely (without unexpected overflow)
			if self.Ntype == SS_DT_SIGNED_NUM {
				if e2int64 < 0 {
					e2int64 = -e2int64 // Ensure non-negative before squaring
				}
				squared := uint64(e2int64) * uint64(e2int64)

				// Check for overflow before addition
				if uint64(self.IntgrVal) > math.MaxUint64-squared {
					return fmt.Errorf("Sumsq: integer overflow detected")
				}

				self.IntgrVal += int64(squared)
				return nil
			}

			// Handle unsigned numbers
			if self.Ntype == SS_DT_UNSIGNED_NUM {
				squared := uint64(e2int64) * uint64(e2int64)

				// Check for overflow before addition
				if uint64(self.IntgrVal) > math.MaxUint64-squared {
					return fmt.Errorf("Sumsq: integer overflow detected")
				}

				self.IntgrVal += int64(squared)
				return nil
			}

			return fmt.Errorf("Sumsq: unsupported type: %v", self.Ntype)
		default:
			return fmt.Errorf("ReduceFast: unsupported int function: %v", fun)
		}
	case SS_DT_FLOAT:
		switch fun {
		case Sum:
			self.FloatVal += e2float64
			return nil
		case Min:
			self.FloatVal = math.Min(self.FloatVal, e2float64)
			return nil
		case Max:
			self.FloatVal = math.Max(self.FloatVal, e2float64)
			return nil
		case Count:
			self.IntgrVal++ // Count should increment an integer counter
			return nil
		case Sumsq:
			squared := e2float64 * e2float64

			// Handle NaN and Infinity cases
			if math.IsNaN(squared) || math.IsInf(squared, 0) {
				return fmt.Errorf("Sumsq: Invalid float operation, result is NaN or Inf")
			}

			self.FloatVal += squared
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
