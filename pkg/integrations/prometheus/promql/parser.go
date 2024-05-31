package promql

import (
	"fmt"

	"github.com/cespare/xxhash"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/results/mresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

func extractSelectors(expr parser.Expr) [][]*labels.Matcher {
	var selectors [][]*labels.Matcher
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		var vs interface{}
		vs, ok := node.(*parser.VectorSelector)
		if ok {
			selectors = append(selectors, vs.(*parser.VectorSelector).LabelMatchers)
		}
		vs, ok = node.(parser.Expressions)
		if ok {
			for _, entry := range vs.(parser.Expressions) {
				expr, ok := entry.(*parser.MatrixSelector)
				if !ok {
					continue
				}
				vectorSelector, ok := expr.VectorSelector.(*parser.VectorSelector)
				if !ok {
					continue
				}
				selectors = append(selectors, vectorSelector.LabelMatchers)
			}
		}
		return nil
	})
	return selectors
}

func extractTimeWindow(args parser.Expressions) (float64, float64, error) {
	if len(args) == 0 {
		return 0, 0, fmt.Errorf("extractTimeWindow: can not extract time window")
	}

	for _, arg := range args {
		if ms, ok := arg.(*parser.MatrixSelector); ok {
			return ms.Range.Seconds(), 0, nil
		} else if subQueryExpr, ok := arg.(*parser.SubqueryExpr); ok {
			return subQueryExpr.Range.Seconds(), subQueryExpr.Step.Seconds(), nil
		}
	}
	return 0, 0, fmt.Errorf("extractTimeWindow: can not extract time window from args: %v", args)
}

func parsePromQLQuery(query string, startTime, endTime uint32, myid uint64) ([]*structs.MetricsQueryRequest, parser.ValueType, []*structs.QueryArithmetic, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		log.Errorf("parsePromQLQuery: Error parsing promql query: %v", err)
		return []*structs.MetricsQueryRequest{}, "", []*structs.QueryArithmetic{}, err
	}

	pqlQuerytype := expr.Type()
	var mQuery structs.MetricsQuery
	mQuery.Aggregator = structs.Aggregation{}
	selectors := extractSelectors(expr)
	//go through labels
	for _, labelEntry := range selectors {
		for _, entry := range labelEntry {
			if entry.Name != "__name__" {
				tagFilter := &structs.TagsFilter{
					TagKey:          entry.Name,
					RawTagValue:     entry.Value,
					HashTagValue:    xxhash.Sum64String(entry.Value),
					TagOperator:     segutils.TagOperator(entry.Type),
					LogicalOperator: segutils.And,
				}
				mQuery.TagsFilters = append(mQuery.TagsFilters, tagFilter)
			} else {
				mQuery.MetricName = entry.Value
			}
		}
	}

	timeRange := &dtu.MetricsTimeRange{
		StartEpochSec: startTime,
		EndEpochSec:   endTime,
	}

	mQuery.OrgId = myid
	mQuery.PqlQueryType = pqlQuerytype

	intervalSeconds, err := mresults.CalculateInterval(endTime - startTime)
	if err != nil {
		return []*structs.MetricsQueryRequest{}, "", []*structs.QueryArithmetic{}, err
	}

	metricQueryRequest := &structs.MetricsQueryRequest{
		MetricsQuery: mQuery,
		TimeRange:    *timeRange,
	}

	mQueryReqs := make([]*structs.MetricsQueryRequest, 0)
	mQueryReqs = append(mQueryReqs, metricQueryRequest)

	queryArithmetic := make([]*structs.QueryArithmetic, 0)
	var exitFromInspect bool

	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		if !exitFromInspect {
			mQueryReqs, queryArithmetic, exitFromInspect, err = parsePromQLExprNode(node, mQueryReqs, queryArithmetic, intervalSeconds)
		}
		return err
	})

	if len(mQueryReqs) == 0 {
		return mQueryReqs, pqlQuerytype, queryArithmetic, nil
	}

	mQuery = mQueryReqs[0].MetricsQuery

	tags := mQuery.TagsFilters
	for idx, tag := range tags {
		var hashedTagVal uint64
		switch v := tag.RawTagValue.(type) {
		case string:
			hashedTagVal = xxhash.Sum64String(v)
		case int64:
			hashedTagVal = uint64(v)
		case float64:
			hashedTagVal = uint64(v)
		case uint64:
			hashedTagVal = v
		default:
			log.Errorf("ParseMetricsRequest: invalid tag value type")
		}
		tags[idx].HashTagValue = hashedTagVal
	}

	if mQuery.MQueryAggs == nil {
		mQuery.MQueryAggs = &structs.MetricQueryAgg{
			AggBlockType:    structs.AggregatorBlock,
			AggregatorBlock: &structs.Aggregation{AggregatorFunction: segutils.Avg},
		}
		mQueryReqs[0].MetricsQuery = mQuery
	} else {
		// If the first Block in the MQueryAggs is not an aggregator block, then add an default aggregator block with avg function

		if mQuery.MQueryAggs.AggBlockType != structs.AggregatorBlock {
			mQuery.MQueryAggs = &structs.MetricQueryAgg{
				AggBlockType:    structs.AggregatorBlock,
				AggregatorBlock: &structs.Aggregation{AggregatorFunction: segutils.Avg},
				Next:            mQuery.MQueryAggs,
			}
			mQueryReqs[0].MetricsQuery = mQuery
		}
	}

	return mQueryReqs, pqlQuerytype, queryArithmetic, nil
}

func parsePromQLExprNode(node parser.Node, mQueryReqs []*structs.MetricsQueryRequest, queryArithmetic []*structs.QueryArithmetic,
	intervalSeconds uint32) ([]*structs.MetricsQueryRequest, []*structs.QueryArithmetic, bool, error) {
	var err error = nil
	var mQueryAgg *structs.MetricQueryAgg
	exit := false
	mQuery := &mQueryReqs[0].MetricsQuery

	if node == nil {
		return mQueryReqs, queryArithmetic, exit, nil
	}

	switch node := node.(type) {
	case *parser.AggregateExpr:
		mQueryAgg, err = handleAggregateExpr(node, mQuery)
		if err == nil {
			updateMetricQueryWithAggs(mQuery, mQueryAgg)
		}
	case *parser.Call:
		mQueryAgg, err = handleCallExpr(node, mQuery)
		if err == nil {
			updateMetricQueryWithAggs(mQuery, mQueryAgg)
		}
	case *parser.VectorSelector:
		mQueryReqs, err = handleVectorSelector(mQueryReqs, intervalSeconds)
	case *parser.BinaryExpr:
		mQueryReqs, queryArithmetic, err = handleBinaryExpr(node, mQueryReqs, queryArithmetic)
		exit = true
	case *parser.MatrixSelector:
		// Process only if the MQueryAggs is nil. Otherwise, it will be handled in the handleCallExpr
		if mQuery.MQueryAggs == nil {
			mQueryReqs[0].TimeRange.StartEpochSec = mQueryReqs[0].TimeRange.EndEpochSec - uint32(node.Range.Seconds())
		}
	case *parser.SubqueryExpr:
		// Process only if the MQueryAggs is nil. Otherwise, it will be handled in the handleCallExpr
		if mQuery.MQueryAggs == nil {
			mQueryReqs, queryArithmetic, exit, err = parsePromQLExprNode(node.Expr, mQueryReqs, queryArithmetic, intervalSeconds)
		}
	case *parser.ParenExpr:
		// Ignore the ParenExpr, As the Expr inside the ParenExpr will be handled in the next iteration
	case *parser.NumberLiteral:
		// Ignore the number literals, As they are handled in the handleCallExpr and BinaryExpr
	default:
		log.Errorf("parsePromQLExprNode: Unsupported node type: %T\n", node)
	}

	if err != nil {
		log.Errorf("parsePromQLExprNode: Error parsing promql query: %v", err)
	}

	return mQueryReqs, queryArithmetic, exit, err
}

func handleAggregateExpr(expr *parser.AggregateExpr, mQuery *structs.MetricsQuery) (*structs.MetricQueryAgg, error) {
	aggFunc := expr.Op.String()

	// Handle parameters if necessary
	if aggFunc == "quantile" && expr.Param != nil {
		if param, ok := expr.Param.(*parser.NumberLiteral); ok {
			mQuery.Aggregator.FuncConstant = param.Val
		}
	}

	switch aggFunc {
	case "avg":
		mQuery.Aggregator.AggregatorFunction = segutils.Avg
	case "count":
		mQuery.Aggregator.AggregatorFunction = segutils.Count
	case "sum":
		mQuery.Aggregator.AggregatorFunction = segutils.Sum
	case "max":
		mQuery.Aggregator.AggregatorFunction = segutils.Max
	case "min":
		mQuery.Aggregator.AggregatorFunction = segutils.Min
	case "quantile":
		mQuery.Aggregator.AggregatorFunction = segutils.Quantile
	case "":
		log.Infof("handleAggregateExpr: using avg aggregator by default for AggregateExpr (got empty string)")
		mQuery.Aggregator = structs.Aggregation{AggregatorFunction: segutils.Avg}
	default:
		return nil, fmt.Errorf("handleAggregateExpr: unsupported aggregation function %v", aggFunc)
	}

	// Handle grouping
	for _, group := range expr.Grouping {
		tagFilter := structs.TagsFilter{
			TagKey:          group,
			RawTagValue:     "*",
			HashTagValue:    xxhash.Sum64String("*"),
			TagOperator:     segutils.TagOperator(segutils.Equal),
			LogicalOperator: segutils.And,
		}
		mQuery.TagsFilters = append(mQuery.TagsFilters, &tagFilter)
	}
	if len(expr.Grouping) > 0 {
		mQuery.Groupby = true
	}

	mQueryAgg := &structs.MetricQueryAgg{
		AggBlockType:    structs.AggregatorBlock,
		AggregatorBlock: mQuery.Aggregator.ShallowClone(),
	}

	return mQueryAgg, nil
}

func handleCallExpr(call *parser.Call, mQuery *structs.MetricsQuery) (*structs.MetricQueryAgg, error) {
	var err error
	defaultCase := false

	for _, arg := range call.Args {
		switch arg := arg.(type) {
		case *parser.MatrixSelector:
			err = handleCallExprMatrixSelectorNode(call, mQuery)
		case *parser.VectorSelector:
			err = handleCallExprVectorSelectorNode(call, mQuery)
		case *parser.ParenExpr:
			err = handleCallExprParenExprNode(call, arg, mQuery)
		case *parser.SubqueryExpr:
			err = handleCallExprMatrixSelectorNode(call, mQuery)
		default:
			defaultCase = true
		}
		if err != nil {
			return nil, fmt.Errorf("handleCallExpr: cannot parse Call Node: %v", err)
		}

		// Break if the case is not default. As Call Expr can have multiple arguments
		// And they will be procesed based on the Node type.
		if !defaultCase {
			break
		}
	}

	if defaultCase {
		// If the default case is true, then args are not of type MatrixSelector or VectorSelector
		// It might be AggregateExpr. And the function can be a Range or Math Function.
		// So, we need to check for both the cases.
		err = handleCallExprVectorSelectorNode(call, mQuery)
		if err != nil {
			err = handleCallExprMatrixSelectorNode(call, mQuery)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("handleCallExpr: cannot parse Call Node: %v", err)
	}

	mQueryAgg := &structs.MetricQueryAgg{
		AggBlockType:  structs.FunctionBlock,
		FunctionBlock: mQuery.Function.ShallowClone(),
	}

	return mQueryAgg, nil
}

func handleCallExprParenExprNode(call *parser.Call, expr *parser.ParenExpr, mQuery *structs.MetricsQuery) error {
	var err error

	switch expr.Expr.(type) {
	case *parser.MatrixSelector:
		err = handleCallExprMatrixSelectorNode(call, mQuery)
	case *parser.VectorSelector:
		err = handleCallExprVectorSelectorNode(call, mQuery)
	case *parser.ParenExpr:
		err = handleCallExprParenExprNode(call, expr.Expr.(*parser.ParenExpr), mQuery)
	}

	return err
}

func handleCallExprMatrixSelectorNode(expr *parser.Call, mQuery *structs.MetricsQuery) error {
	function := expr.Func.Name

	timeWindow, step, err := extractTimeWindow(expr.Args)
	if err != nil {
		return fmt.Errorf("handleCallExprMatrixSelectorNode: cannot extract time window: %v", err)
	}

	if mQuery.TagsFilters != nil {
		mQuery.Groupby = true
	}

	return handlePromQLRangeFunctionNode(function, timeWindow, step, expr, mQuery)
}

func handlePromQLRangeFunctionNode(functionName string, timeWindow, step float64, expr *parser.Call, mQuery *structs.MetricsQuery) error {
	switch functionName {
	case "deriv":
		mQuery.Function = structs.Function{RangeFunction: segutils.Derivative, TimeWindow: timeWindow, Step: step}
	case "delta":
		mQuery.Function = structs.Function{RangeFunction: segutils.Delta, TimeWindow: timeWindow, Step: step}
	case "idelta":
		mQuery.Function = structs.Function{RangeFunction: segutils.IDelta, TimeWindow: timeWindow, Step: step}
	case "rate":
		mQuery.Function = structs.Function{RangeFunction: segutils.Rate, TimeWindow: timeWindow, Step: step}
	case "irate":
		mQuery.Function = structs.Function{RangeFunction: segutils.IRate, TimeWindow: timeWindow, Step: step}
	case "increase":
		mQuery.Function = structs.Function{RangeFunction: segutils.Increase, TimeWindow: timeWindow, Step: step}
	case "avg_over_time":
		mQuery.Function = structs.Function{RangeFunction: segutils.Avg_Over_Time, TimeWindow: timeWindow, Step: step}
	case "min_over_time":
		mQuery.Function = structs.Function{RangeFunction: segutils.Min_Over_Time, TimeWindow: timeWindow, Step: step}
	case "max_over_time":
		mQuery.Function = structs.Function{RangeFunction: segutils.Max_Over_Time, TimeWindow: timeWindow, Step: step}
	case "sum_over_time":
		mQuery.Function = structs.Function{RangeFunction: segutils.Sum_Over_Time, TimeWindow: timeWindow, Step: step}
	case "count_over_time":
		mQuery.Function = structs.Function{RangeFunction: segutils.Count_Over_Time, TimeWindow: timeWindow, Step: step}
	case "stdvar_over_time":
		mQuery.Function = structs.Function{RangeFunction: segutils.Stdvar_Over_Time, TimeWindow: timeWindow}
	case "stddev_over_time":
		mQuery.Function = structs.Function{RangeFunction: segutils.Stddev_Over_Time, TimeWindow: timeWindow}
	case "last_over_time":
		mQuery.Function = structs.Function{RangeFunction: segutils.Last_Over_Time, TimeWindow: timeWindow}
	case "present_over_time":
		mQuery.Function = structs.Function{RangeFunction: segutils.Present_Over_Time, TimeWindow: timeWindow}
	case "quantile_over_time":
		if len(expr.Args) != 2 {
			return fmt.Errorf("parser.Inspect: Incorrect parameters: %v for the quantile_over_time function", expr.Args.String())
		}
		mQuery.Function = structs.Function{RangeFunction: segutils.Quantile_Over_Time, TimeWindow: timeWindow, ValueList: []string{expr.Args[0].String()}}
	case "changes":
		mQuery.Function = structs.Function{RangeFunction: segutils.Changes, TimeWindow: timeWindow, Step: step}
	case "resets":
		mQuery.Function = structs.Function{RangeFunction: segutils.Resets, TimeWindow: timeWindow, Step: step}
	default:
		return fmt.Errorf("handlePromQLRangeFunctionNode: unsupported function type %v", functionName)
	}

	return nil
}

func handleCallExprVectorSelectorNode(expr *parser.Call, mQuery *structs.MetricsQuery) error {
	function := expr.Func.Name

	switch function {
	case "abs":
		mQuery.Function = structs.Function{MathFunction: segutils.Abs}
	case "sqrt":
		mQuery.Function = structs.Function{MathFunction: segutils.Sqrt}
	case "ceil":
		mQuery.Function = structs.Function{MathFunction: segutils.Ceil}
	case "round":
		mQuery.Function = structs.Function{MathFunction: segutils.Round}
		if len(expr.Args) > 1 {
			mQuery.Function.ValueList = []string{expr.Args[1].String()}
		}
	case "floor":
		mQuery.Function = structs.Function{MathFunction: segutils.Floor}
	case "exp":
		mQuery.Function = structs.Function{MathFunction: segutils.Exp}
	case "ln":
		mQuery.Function = structs.Function{MathFunction: segutils.Ln}
	case "log2":
		mQuery.Function = structs.Function{MathFunction: segutils.Log2}
	case "log10":
		mQuery.Function = structs.Function{MathFunction: segutils.Log10}
	case "sgn":
		mQuery.Function = structs.Function{MathFunction: segutils.Sgn}
	case "deg":
		mQuery.Function = structs.Function{MathFunction: segutils.Deg}
	case "rad":
		mQuery.Function = structs.Function{MathFunction: segutils.Rad}
	case "acos":
		mQuery.Function = structs.Function{MathFunction: segutils.Acos}
	case "acosh":
		mQuery.Function = structs.Function{MathFunction: segutils.Acosh}
	case "asin":
		mQuery.Function = structs.Function{MathFunction: segutils.Asin}
	case "asinh":
		mQuery.Function = structs.Function{MathFunction: segutils.Asinh}
	case "atan":
		mQuery.Function = structs.Function{MathFunction: segutils.Atan}
	case "atanh":
		mQuery.Function = structs.Function{MathFunction: segutils.Atanh}
	case "cos":
		mQuery.Function = structs.Function{MathFunction: segutils.Cos}
	case "cosh":
		mQuery.Function = structs.Function{MathFunction: segutils.Cosh}
	case "sin":
		mQuery.Function = structs.Function{MathFunction: segutils.Sin}
	case "sinh":
		mQuery.Function = structs.Function{MathFunction: segutils.Sinh}
	case "tan":
		mQuery.Function = structs.Function{MathFunction: segutils.Tan}
	case "tanh":
		mQuery.Function = structs.Function{MathFunction: segutils.Tanh}
	case "clamp":
		if len(expr.Args) != 3 {
			return fmt.Errorf("handleCallExprVectorSelectorNode: incorrect parameters: %v for the clamp function", expr.Args.String())
		}
		mQuery.Function = structs.Function{MathFunction: segutils.Clamp, ValueList: []string{expr.Args[1].String(), expr.Args[2].String()}}
	case "clamp_max":
		if len(expr.Args) != 2 {
			return fmt.Errorf("handleCallExprVectorSelectorNode: incorrect parameters: %v for the clamp_max function", expr.Args.String())
		}
		mQuery.Function = structs.Function{MathFunction: segutils.Clamp_Max, ValueList: []string{expr.Args[1].String()}}
	case "clamp_min":
		if len(expr.Args) != 2 {
			return fmt.Errorf("handleCallExprVectorSelectorNode: incorrect parameters: %v for the clamp_min function", expr.Args.String())
		}
		mQuery.Function = structs.Function{MathFunction: segutils.Clamp_Min, ValueList: []string{expr.Args[1].String()}}
	case "timestamp":
		mQuery.Function = structs.Function{MathFunction: segutils.Timestamp}
	case "hour":
		mQuery.Function = structs.Function{TimeFunction: segutils.Hour}
	case "minute":
		mQuery.Function = structs.Function{TimeFunction: segutils.Minute}
	case "month":
		mQuery.Function = structs.Function{TimeFunction: segutils.Month}
	case "year":
		mQuery.Function = structs.Function{TimeFunction: segutils.Year}
	case "day_of_month":
		mQuery.Function = structs.Function{TimeFunction: segutils.DayOfMonth}
	case "day_of_week":
		mQuery.Function = structs.Function{TimeFunction: segutils.DayOfWeek}
	case "day_of_year":
		mQuery.Function = structs.Function{TimeFunction: segutils.DayOfYear}
	case "days_in_month":
		mQuery.Function = structs.Function{TimeFunction: segutils.DaysInMonth}
	default:
		return fmt.Errorf("handleCallExprVectorSelectorNode: unsupported function type %v", function)
	}

	return nil
}

func handleVectorSelector(mQueryReqs []*structs.MetricsQueryRequest, intervalSeconds uint32) ([]*structs.MetricsQueryRequest, error) {
	mQuery := &mQueryReqs[0].MetricsQuery
	mQuery.HashedMName = xxhash.Sum64String(mQuery.MetricName)
	mQuery.SelectAllSeries = true
	agg := structs.Aggregation{AggregatorFunction: segutils.Avg}
	mQuery.Downsampler = structs.Downsampler{Interval: int(intervalSeconds), Unit: "s", Aggregator: agg}

	if len(mQuery.TagsFilters) > 0 {
		mQuery.SelectAllSeries = false
	} else {
		mQuery.SelectAllSeries = true
	}

	return mQueryReqs, nil
}

func handleBinaryExpr(expr *parser.BinaryExpr, mQueryReqs []*structs.MetricsQueryRequest,
	queryArithmetic []*structs.QueryArithmetic) ([]*structs.MetricsQueryRequest, []*structs.QueryArithmetic, error) {

	var err error

	myid := mQueryReqs[0].MetricsQuery.OrgId
	timeRange := mQueryReqs[0].TimeRange

	arithmeticOperation := structs.QueryArithmetic{}
	var lhsRequest, rhsRequest []*structs.MetricsQueryRequest
	var lhsQueryArth, rhsQueryArth []*structs.QueryArithmetic

	if constant, ok := expr.LHS.(*parser.NumberLiteral); ok {
		arithmeticOperation.ConstantOp = true
		arithmeticOperation.Constant = constant.Val
	} else {
		lhsRequest, _, lhsQueryArth, err = parsePromQLQuery(expr.LHS.String(), timeRange.StartEpochSec, timeRange.EndEpochSec, myid)
		if err != nil {
			return mQueryReqs, queryArithmetic, err
		}
		arithmeticOperation.LHS = lhsRequest[0].MetricsQuery.HashedMName
		queryArithmetic = append(queryArithmetic, lhsQueryArth...)
	}

	if constant, ok := expr.RHS.(*parser.NumberLiteral); ok {
		arithmeticOperation.ConstantOp = true
		arithmeticOperation.Constant = constant.Val
	} else {
		rhsRequest, _, rhsQueryArth, err = parsePromQLQuery(expr.RHS.String(), timeRange.StartEpochSec, timeRange.EndEpochSec, myid)
		if err != nil {
			return mQueryReqs, queryArithmetic, err
		}
		arithmeticOperation.RHS = rhsRequest[0].MetricsQuery.HashedMName
		queryArithmetic = append(queryArithmetic, rhsQueryArth...)
	}
	arithmeticOperation.Operation = getLogicalAndArithmeticOperation(expr.Op)
	queryArithmetic = append(queryArithmetic, &arithmeticOperation)

	if mQueryReqs[0].MetricsQuery.MQueryAggs == nil {
		mQueryReqs = lhsRequest
	} else if mQueryReqs[0].MetricsQuery.HashedMName == uint64(0) {
		// This means there is a common group by on multiple metrics separated by operators.
		// So, we need to append the aggregations to each of the Request in LHS and RHS.
		// And we can discard the current aggregation in the mQueryReqs[0]
		rhsRequest = appendMetricAggsToTheMQuery(rhsRequest, &mQueryReqs[0].MetricsQuery)
		mQueryReqs = appendMetricAggsToTheMQuery(lhsRequest, &mQueryReqs[0].MetricsQuery)
	} else {
		mQueryReqs = append(mQueryReqs, lhsRequest...)
	}
	mQueryReqs = append(mQueryReqs, rhsRequest...)

	return mQueryReqs, queryArithmetic, nil
}

func appendMetricAggsToTheMQuery(mQueryReqs []*structs.MetricsQueryRequest, mQuery *structs.MetricsQuery) []*structs.MetricsQueryRequest {
	for _, mQueryReq := range mQueryReqs {
		currentAggs := mQueryReq.MetricsQuery.MQueryAggs
		for currentAggs.Next != nil {
			currentAggs = currentAggs.Next
		}
		currentAggs.Next = mQuery.MQueryAggs

		mQueryReq.MetricsQuery.Groupby = mQuery.Groupby
		mQueryReq.MetricsQuery.SelectAllSeries = mQuery.SelectAllSeries
		mQueryReq.MetricsQuery.TagsFilters = mQuery.TagsFilters
	}
	return mQueryReqs
}

func updateMetricQueryWithAggs(mQuery *structs.MetricsQuery, mQueryAgg *structs.MetricQueryAgg) {

	// If MQueryAggs is nil, set it to the new MetricQueryAgg
	if mQuery.MQueryAggs == nil {
		mQuery.MQueryAggs = mQueryAgg
	} else {
		// Otherwise, set the new MetricQueryAgg to the head of the chain
		// and set the current head as the next of the new MetricQueryAgg
		mQueryAgg.Next = mQuery.MQueryAggs
		mQuery.MQueryAggs = mQueryAgg
	}

	// Reset the Function And Aggregator fields to handle the next function call correctly
	mQuery.Function = structs.Function{}
	mQuery.Aggregator = structs.Aggregation{}
}
