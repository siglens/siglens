package promql

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/cespare/xxhash"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/siglens/siglens/pkg/common/dtypeutils"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	putils "github.com/siglens/siglens/pkg/integrations/prometheus/utils"
	"github.com/siglens/siglens/pkg/segment/results/mresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
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

func ConvertPromQLToMetricsQuery(query string, startTime, endTime uint32, myid int64) ([]structs.MetricsQueryRequest, parser.ValueType, []structs.QueryArithmetic, error) {
	// Check if the query is just a number
	_, err := dtypeutils.ConvertToFloat(query, 64)
	if err == nil {
		// If yes, then add 0 + to the query, to convert it as a Binary Expr
		query = fmt.Sprintf("0 + %s", query)
	}
	mQueryReqs, pqlQuerytype, queryArithmetic, err := parsePromQLQuery(query, startTime, endTime, myid)
	if err != nil {
		return []structs.MetricsQueryRequest{}, "", []structs.QueryArithmetic{}, err
	}

	metricQueryRequests := make([]structs.MetricsQueryRequest, 0)
	for _, mQueryReq := range mQueryReqs {
		metricQueryRequests = append(metricQueryRequests, *mQueryReq)
	}

	queryArithmetics := make([]structs.QueryArithmetic, 0)
	for _, queryArithmetic := range queryArithmetic {
		queryArithmetics = append(queryArithmetics, *queryArithmetic)
	}

	return metricQueryRequests, pqlQuerytype, queryArithmetics, nil
}

func parsePromQLQuery(query string, startTime, endTime uint32, myid int64) ([]*structs.MetricsQueryRequest, parser.ValueType, []*structs.QueryArithmetic, error) {
	parser.EnableExperimentalFunctions = true
	expr, err := parser.ParseExpr(query)
	if err != nil {
		log.Errorf("parsePromQLQuery: Error parsing promql query: %v", err)
		return []*structs.MetricsQueryRequest{}, "", []*structs.QueryArithmetic{}, err
	}

	pqlQuerytype := expr.Type()
	var mQuery structs.MetricsQuery
	mQuery.FirstAggregator = structs.Aggregation{}
	selectors := extractSelectors(expr)
	//go through labels
	for _, labelEntry := range selectors {
		for _, entry := range labelEntry {
			if entry.Name != "__name__" {
				tagFilter := &structs.TagsFilter{
					TagKey:          entry.Name,
					RawTagValue:     entry.Value,
					HashTagValue:    xxhash.Sum64String(entry.Value),
					TagOperator:     sutils.TagOperator(entry.Type),
					LogicalOperator: sutils.And,
				}
				mQuery.TagsFilters = append(mQuery.TagsFilters, tagFilter)
			} else {
				mQuery.MetricOperator = sutils.TagOperator(entry.Type)
				mQuery.MetricName = entry.Value

				if mQuery.IsRegexOnMetricName() {
					// If the metric name is a regex, then we need to add the start and end anchors
					anchoredMetricName := fmt.Sprintf("^(%v)$", entry.Value)
					_, err := regexp.Compile(anchoredMetricName)
					if err != nil {
						log.Errorf("parsePromQLQuery: Error compiling regex for the anchored MetricName Pattern: %v. Error=%v", anchoredMetricName, err)
						return []*structs.MetricsQueryRequest{}, "", []*structs.QueryArithmetic{}, err
					}
					mQuery.MetricNameRegexPattern = anchoredMetricName
				}
			}
		}
	}

	timeRange := &dtu.MetricsTimeRange{
		StartEpochSec: startTime,
		EndEpochSec:   endTime,
	}

	mQuery.OrgId = myid
	mQuery.PqlQueryType = pqlQuerytype
	mQuery.QueryHash = xxhash.Sum64String(query)

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

	if err != nil {
		return []*structs.MetricsQueryRequest{}, "", []*structs.QueryArithmetic{}, err
	}

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

	if mQuery.SubsequentAggs == nil {
		mQuery.SubsequentAggs = &structs.MetricQueryAgg{
			AggBlockType:    structs.AggregatorBlock,
			AggregatorBlock: &structs.Aggregation{AggregatorFunction: sutils.Avg, Without: true},
		}
		if len(mQuery.TagsFilters) == 0 {
			mQuery.GetAllLabels = true
		}
		mQueryReqs[0].MetricsQuery = mQuery
	} else {
		// If the first Block in the MQueryAggs is not an aggregator block, then add an default aggregator block with avg function

		if mQuery.SubsequentAggs.AggBlockType != structs.AggregatorBlock {
			mQuery.SubsequentAggs = &structs.MetricQueryAgg{
				AggBlockType:    structs.AggregatorBlock,
				AggregatorBlock: &structs.Aggregation{AggregatorFunction: sutils.Avg, Without: true},
				Next:            mQuery.SubsequentAggs,
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
		mQueryAgg, err = handleCallExpr(node, mQueryReqs[0])
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
		if mQuery.SubsequentAggs == nil {
			mQueryReqs[0].TimeRange.StartEpochSec = mQueryReqs[0].TimeRange.EndEpochSec - uint32(node.Range.Seconds())
		}
	case *parser.SubqueryExpr:
		// Process only if the MQueryAggs is nil. Otherwise, it will be handled in the handleCallExpr
		if mQuery.SubsequentAggs == nil {
			mQueryReqs, queryArithmetic, exit, err = parsePromQLExprNode(node.Expr, mQueryReqs, queryArithmetic, intervalSeconds)
		}
	case *parser.ParenExpr:
		// Ignore the ParenExpr, As the Expr inside the ParenExpr will be handled in the next iteration
	case *parser.NumberLiteral:
		// Ignore the number literals, As they are handled in the handleCallExpr and BinaryExpr
	case *parser.StringLiteral:
		// Ignore the string literals, As they are handled in the handleCallExpr and other functions
	default:
		log.Errorf("parsePromQLExprNode: Unsupported node type: %T\n", node)
	}

	if err != nil {
		log.Errorf("parsePromQLExprNode: Error parsing promql query: %v", err)
	}

	return mQueryReqs, queryArithmetic, exit, err
}

// To check if the current Expr or nested Expr contains a AggregateExpr
func hasNestedAggregateExpr(expr parser.Expr) bool {
	var isAggregateExpr bool

	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if _, ok := node.(*parser.AggregateExpr); ok {
			isAggregateExpr = true
			return fmt.Errorf("hasNestedAggregateExpr: Found AggregateExpr") // Break the Inspect
		}
		return nil
	})

	return isAggregateExpr
}

func handleAggregateExpr(expr *parser.AggregateExpr, mQuery *structs.MetricsQuery) (*structs.MetricQueryAgg, error) {
	aggFunc := expr.Op.String()

	// Handle parameters if necessary
	if aggFunc == "quantile" && expr.Param != nil {
		if param, ok := expr.Param.(*parser.NumberLiteral); ok {
			mQuery.FirstAggregator.FuncConstant = param.Val
		}
	}

	switch aggFunc {
	case "avg":
		mQuery.FirstAggregator.AggregatorFunction = sutils.Avg
	case "count":
		mQuery.FirstAggregator.AggregatorFunction = sutils.Count
		mQuery.GetAllLabels = true
	case "sum":
		mQuery.FirstAggregator.AggregatorFunction = sutils.Sum
	case "max":
		mQuery.FirstAggregator.AggregatorFunction = sutils.Max
	case "min":
		mQuery.FirstAggregator.AggregatorFunction = sutils.Min
	case "quantile":
		mQuery.FirstAggregator.AggregatorFunction = sutils.Quantile
	case "topk":
		numberLiteral, ok := expr.Param.(*parser.NumberLiteral)
		if !ok {
			return nil, fmt.Errorf("handleAggregateExpr: topk contains invalid param: %v", expr.Param)
		}
		mQuery.FirstAggregator.AggregatorFunction = sutils.TopK
		mQuery.FirstAggregator.FuncConstant = numberLiteral.Val
		mQuery.GetAllLabels = true
	case "bottomk":
		numberLiteral, ok := expr.Param.(*parser.NumberLiteral)
		if !ok {
			return nil, fmt.Errorf("handleAggregateExpr: bottomk contains invalid param: %v", expr.Param)
		}
		mQuery.FirstAggregator.AggregatorFunction = sutils.BottomK
		mQuery.FirstAggregator.FuncConstant = numberLiteral.Val
		mQuery.GetAllLabels = true
	case "stddev":
		mQuery.FirstAggregator.AggregatorFunction = sutils.Stddev
		mQuery.GetAllLabels = true
	case "stdvar":
		mQuery.FirstAggregator.AggregatorFunction = sutils.Stdvar
		mQuery.GetAllLabels = true
	case "group":
		mQuery.FirstAggregator.AggregatorFunction = sutils.Group
	case "":
		log.Infof("handleAggregateExpr: using avg aggregator by default for AggregateExpr (got empty string)")
		mQuery.FirstAggregator = structs.Aggregation{AggregatorFunction: sutils.Avg}
	default:
		return nil, fmt.Errorf("handleAggregateExpr: unsupported aggregation function %v", aggFunc)
	}

	// if True, it implies that there is a nested AggregateExpr in the current Expr
	// And this group by Aggregation should not be done on the initial Aggregation.
	hasAggExpr := hasNestedAggregateExpr(expr.Expr)

	if len(expr.Grouping) > 0 {
		mQuery.Groupby = true
	} else {
		mQuery.AggWithoutGroupBy = true
		mQuery.SelectAllSeries = mQuery.GetAllLabels
	}

	mQuery.FirstAggregator.GroupByFields = sort.StringSlice(expr.Grouping)
	mQuery.FirstAggregator.Without = expr.Without

	if expr.Without {
		mQuery.SelectAllSeries = true
		mQuery.GetAllLabels = true
	}

	// Handle grouping
	for _, group := range expr.Grouping {
		if group == "__name__" {
			mQuery.GroupByMetricName = true
			continue
		}
		tagFilter := structs.TagsFilter{
			TagKey:          group,
			RawTagValue:     "*",
			HashTagValue:    xxhash.Sum64String("*"),
			TagOperator:     sutils.TagOperator(sutils.Equal),
			LogicalOperator: sutils.And,
			NotInitialGroup: hasAggExpr,
			IgnoreTag:       expr.Without,
			IsGroupByKey:    true,
		}
		mQuery.TagsFilters = append(mQuery.TagsFilters, &tagFilter)
	}

	mQueryAgg := &structs.MetricQueryAgg{
		AggBlockType:    structs.AggregatorBlock,
		AggregatorBlock: mQuery.FirstAggregator.ShallowClone(),
	}

	return mQueryAgg, nil
}

func handleCallExpr(call *parser.Call, mQueryReq *structs.MetricsQueryRequest) (*structs.MetricQueryAgg, error) {
	var err error
	defaultCase := false

	mQuery := &mQueryReq.MetricsQuery

	for _, arg := range call.Args {
		switch arg := arg.(type) {
		case *parser.MatrixSelector:
			err = handleCallExprMatrixSelectorNode(call, mQueryReq)
		case *parser.VectorSelector:
			err = handleCallExprVectorSelectorNode(call, mQuery)
		case *parser.ParenExpr:
			err = handleCallExprParenExprNode(call, arg, mQueryReq)
		case *parser.SubqueryExpr:
			err = handleCallExprMatrixSelectorNode(call, mQueryReq)
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
			err = handleCallExprMatrixSelectorNode(call, mQueryReq)
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

func handleCallExprParenExprNode(call *parser.Call, expr *parser.ParenExpr, mQueryReq *structs.MetricsQueryRequest) error {
	var err error

	mQuery := &mQueryReq.MetricsQuery

	switch expr.Expr.(type) {
	case *parser.MatrixSelector:
		err = handleCallExprMatrixSelectorNode(call, mQueryReq)
	case *parser.VectorSelector:
		err = handleCallExprVectorSelectorNode(call, mQuery)
	case *parser.ParenExpr:
		err = handleCallExprParenExprNode(call, expr.Expr.(*parser.ParenExpr), mQueryReq)
	}

	return err
}

func handleCallExprMatrixSelectorNode(expr *parser.Call, mQueryReq *structs.MetricsQueryRequest) error {
	function := expr.Func.Name

	timeWindow, step, err := extractTimeWindow(expr.Args)
	if err != nil {
		return fmt.Errorf("handleCallExprMatrixSelectorNode: cannot extract time window: %v", err)
	}

	mQuery := &mQueryReq.MetricsQuery

	if len(mQuery.TagsFilters) > 0 {
		if mQuery.Groupby {
			// If group by is already set, then the tagFilters that are added because of the group by should not be initial group
			// As this grouping affects this range function group by
			for _, tag := range mQuery.TagsFilters {
				if tag.IsGroupByKey {
					tag.NotInitialGroup = true
				}
			}
		}

		mQuery.Groupby = true
	}

	if step == 0 {
		step = getStepValueFromTimeRange(&mQueryReq.TimeRange)
	}

	return handlePromQLRangeFunctionNode(function, timeWindow, step, expr, mQuery)
}

func handlePromQLRangeFunctionNode(functionName string, timeWindow, step float64, expr *parser.Call, mQuery *structs.MetricsQuery) error {
	switch functionName {
	case "deriv":
		mQuery.Function = structs.Function{RangeFunction: sutils.Derivative, TimeWindow: timeWindow, Step: step}
	case "predict_linear":
		if len(expr.Args) != 2 {
			return fmt.Errorf("parser.Inspect: Incorrect parameters: %v for the predict_linear function", expr.Args.String())
		}
		mQuery.Function = structs.Function{RangeFunction: sutils.Predict_Linear, TimeWindow: timeWindow, ValueList: []string{expr.Args[1].String()}}
	case "delta":
		mQuery.Function = structs.Function{RangeFunction: sutils.Delta, TimeWindow: timeWindow, Step: step}
	case "idelta":
		mQuery.Function = structs.Function{RangeFunction: sutils.IDelta, TimeWindow: timeWindow, Step: step}
	case "rate":
		mQuery.Function = structs.Function{RangeFunction: sutils.Rate, TimeWindow: timeWindow, Step: step}
	case "irate":
		mQuery.Function = structs.Function{RangeFunction: sutils.IRate, TimeWindow: timeWindow, Step: step}
	case "increase":
		mQuery.Function = structs.Function{RangeFunction: sutils.Increase, TimeWindow: timeWindow, Step: step}
	case "avg_over_time":
		mQuery.Function = structs.Function{RangeFunction: sutils.Avg_Over_Time, TimeWindow: timeWindow, Step: step}
	case "min_over_time":
		mQuery.Function = structs.Function{RangeFunction: sutils.Min_Over_Time, TimeWindow: timeWindow, Step: step}
	case "max_over_time":
		mQuery.Function = structs.Function{RangeFunction: sutils.Max_Over_Time, TimeWindow: timeWindow, Step: step}
	case "sum_over_time":
		mQuery.Function = structs.Function{RangeFunction: sutils.Sum_Over_Time, TimeWindow: timeWindow, Step: step}
	case "count_over_time":
		mQuery.Function = structs.Function{RangeFunction: sutils.Count_Over_Time, TimeWindow: timeWindow, Step: step}
	case "stdvar_over_time":
		mQuery.Function = structs.Function{RangeFunction: sutils.Stdvar_Over_Time, TimeWindow: timeWindow, Step: step}
	case "stddev_over_time":
		mQuery.Function = structs.Function{RangeFunction: sutils.Stddev_Over_Time, TimeWindow: timeWindow, Step: step}
	case "last_over_time":
		mQuery.Function = structs.Function{RangeFunction: sutils.Last_Over_Time, TimeWindow: timeWindow, Step: step}
	case "present_over_time":
		mQuery.Function = structs.Function{RangeFunction: sutils.Present_Over_Time, TimeWindow: timeWindow, Step: step}
	case "mad_over_time":
		mQuery.Function = structs.Function{RangeFunction: sutils.Mad_Over_Time, TimeWindow: timeWindow, Step: step}
	case "quantile_over_time":
		if len(expr.Args) != 2 {
			return fmt.Errorf("parser.Inspect: Incorrect parameters: %v for the quantile_over_time function", expr.Args.String())
		}
		mQuery.Function = structs.Function{RangeFunction: sutils.Quantile_Over_Time, TimeWindow: timeWindow, ValueList: []string{expr.Args[0].String()}, Step: step}
	case "changes":
		mQuery.Function = structs.Function{RangeFunction: sutils.Changes, TimeWindow: timeWindow, Step: step}
	case "resets":
		mQuery.Function = structs.Function{RangeFunction: sutils.Resets, TimeWindow: timeWindow, Step: step}
	default:
		return fmt.Errorf("handlePromQLRangeFunctionNode: unsupported function type %v", functionName)
	}

	mQuery.LookBackToInclude = timeWindow
	mQuery.SelectAllSeries = true
	mQuery.GetAllLabels = true

	return nil
}

func handleCallExprVectorSelectorNode(expr *parser.Call, mQuery *structs.MetricsQuery) error {
	function := expr.Func.Name

	switch function {
	case "abs":
		mQuery.Function = structs.Function{MathFunction: sutils.Abs}
	case "sqrt":
		mQuery.Function = structs.Function{MathFunction: sutils.Sqrt}
	case "ceil":
		mQuery.Function = structs.Function{MathFunction: sutils.Ceil}
	case "round":
		mQuery.Function = structs.Function{MathFunction: sutils.Round}
		if len(expr.Args) > 1 {
			mQuery.Function.ValueList = []string{expr.Args[1].String()}
		}
	case "floor":
		mQuery.Function = structs.Function{MathFunction: sutils.Floor}
	case "exp":
		mQuery.Function = structs.Function{MathFunction: sutils.Exp}
	case "ln":
		mQuery.Function = structs.Function{MathFunction: sutils.Ln}
	case "log2":
		mQuery.Function = structs.Function{MathFunction: sutils.Log2}
	case "log10":
		mQuery.Function = structs.Function{MathFunction: sutils.Log10}
	case "sgn":
		mQuery.Function = structs.Function{MathFunction: sutils.Sgn}
	case "deg":
		mQuery.Function = structs.Function{MathFunction: sutils.Deg}
	case "rad":
		mQuery.Function = structs.Function{MathFunction: sutils.Rad}
	case "acos":
		mQuery.Function = structs.Function{MathFunction: sutils.Acos}
	case "acosh":
		mQuery.Function = structs.Function{MathFunction: sutils.Acosh}
	case "asin":
		mQuery.Function = structs.Function{MathFunction: sutils.Asin}
	case "asinh":
		mQuery.Function = structs.Function{MathFunction: sutils.Asinh}
	case "atan":
		mQuery.Function = structs.Function{MathFunction: sutils.Atan}
	case "atanh":
		mQuery.Function = structs.Function{MathFunction: sutils.Atanh}
	case "cos":
		mQuery.Function = structs.Function{MathFunction: sutils.Cos}
	case "cosh":
		mQuery.Function = structs.Function{MathFunction: sutils.Cosh}
	case "sin":
		mQuery.Function = structs.Function{MathFunction: sutils.Sin}
	case "sinh":
		mQuery.Function = structs.Function{MathFunction: sutils.Sinh}
	case "tan":
		mQuery.Function = structs.Function{MathFunction: sutils.Tan}
	case "tanh":
		mQuery.Function = structs.Function{MathFunction: sutils.Tanh}
	case "clamp":
		if len(expr.Args) != 3 {
			return fmt.Errorf("handleCallExprVectorSelectorNode: incorrect parameters: %v for the clamp function", expr.Args.String())
		}
		mQuery.Function = structs.Function{MathFunction: sutils.Clamp, ValueList: []string{expr.Args[1].String(), expr.Args[2].String()}}
	case "clamp_max":
		if len(expr.Args) != 2 {
			return fmt.Errorf("handleCallExprVectorSelectorNode: incorrect parameters: %v for the clamp_max function", expr.Args.String())
		}
		mQuery.Function = structs.Function{MathFunction: sutils.Clamp_Max, ValueList: []string{expr.Args[1].String()}}
	case "clamp_min":
		if len(expr.Args) != 2 {
			return fmt.Errorf("handleCallExprVectorSelectorNode: incorrect parameters: %v for the clamp_min function", expr.Args.String())
		}
		mQuery.Function = structs.Function{MathFunction: sutils.Clamp_Min, ValueList: []string{expr.Args[1].String()}}
	case "timestamp":
		mQuery.Function = structs.Function{MathFunction: sutils.Timestamp}
	case "hour":
		mQuery.Function = structs.Function{TimeFunction: sutils.Hour}
	case "minute":
		mQuery.Function = structs.Function{TimeFunction: sutils.Minute}
	case "month":
		mQuery.Function = structs.Function{TimeFunction: sutils.Month}
	case "year":
		mQuery.Function = structs.Function{TimeFunction: sutils.Year}
	case "day_of_month":
		mQuery.Function = structs.Function{TimeFunction: sutils.DayOfMonth}
	case "day_of_week":
		mQuery.Function = structs.Function{TimeFunction: sutils.DayOfWeek}
	case "day_of_year":
		mQuery.Function = structs.Function{TimeFunction: sutils.DayOfYear}
	case "days_in_month":
		mQuery.Function = structs.Function{TimeFunction: sutils.DaysInMonth}
	case "label_replace":
		if len(expr.Args) != 5 {
			return fmt.Errorf("handleCallExprVectorSelectorNode: incorrect parameters: %v for the label_replace function", expr.Args.String())
		}

		rawRegexStrLiteral, ok := expr.Args[4].(*parser.StringLiteral)
		if !ok {
			return fmt.Errorf("handleCallExprVectorSelectorNode: incorrect parameters:%v. Expected the regex to be a string", expr.Args.String())
		}

		rawRegex := rawRegexStrLiteral.Val

		gobRegexp := &utils.GobbableRegex{}
		err := gobRegexp.SetRegex(rawRegex)
		if err != nil {
			return fmt.Errorf("handleCallExprVectorSelectorNode: Error compiling regex for the GobRegex Pattern: %v. Error=%v", rawRegex, err)
		}

		replacementKeyLietral, ok := expr.Args[2].(*parser.StringLiteral)
		if !ok {
			return fmt.Errorf("handleCallExprVectorSelectorNode: incorrect parameters:%v. Expected the replacement key to be a string", expr.Args.String())
		}

		replacementKey := replacementKeyLietral.Val

		if replacementKey == "" {
			return fmt.Errorf("handleCallExprVectorSelectorNode: replacement key cannot be empty")
		}

		key := strings.TrimPrefix(replacementKey, "$")
		if key == "" {
			return fmt.Errorf("handleCallExprVectorSelectorNode: replacement key cannot be empty")
		}

		labelReplacementKey := &structs.LabelReplacementKey{}

		intVal, err := strconv.Atoi(key)
		if err == nil {
			labelReplacementKey.KeyType = structs.IndexBased
			labelReplacementKey.IndexBasedVal = intVal
		} else {
			labelReplacementKey.KeyType = structs.NameBased
			labelReplacementKey.NameBasedVal = key
		}

		destinationLabelLiteral, ok := expr.Args[1].(*parser.StringLiteral)
		if !ok {
			return fmt.Errorf("handleCallExprVectorSelectorNode: incorrect parameters:%v. Expected the destination label to be a string", expr.Args.String())
		}

		sourceLabelLiteral, ok := expr.Args[3].(*parser.StringLiteral)
		if !ok {
			return fmt.Errorf("handleCallExprVectorSelectorNode: incorrect parameters:%v. Expected the source label to be a string", expr.Args.String())
		}

		mQuery.Function = structs.Function{
			FunctionType: structs.LabelFunction,
			LabelFunction: &structs.LabelFunctionExpr{
				FunctionType:     sutils.LabelReplace,
				DestinationLabel: destinationLabelLiteral.Val,
				Replacement:      labelReplacementKey,
				SourceLabel:      sourceLabelLiteral.Val,
				GobRegexp:        gobRegexp,
			},
		}
	case "label_join":
		// TODO: Implement label_join function
		return fmt.Errorf("handleCallExprVectorSelectorNode: label_join function is not supported")
	case "histogram_quantile":
		if len(expr.Args) != 2 {
			return fmt.Errorf("handleCallExprVectorSelectorNode: incorrect parameters: %v for the histogram_quantile function", expr.Args.String())
		}

		quantileLiteral, ok := expr.Args[0].(*parser.NumberLiteral)
		if !ok {
			return fmt.Errorf("handleCallExprVectorSelectorNode: incorrect parameters:%v. Expected the quantile to be a number", expr.Args.String())
		}

		mQuery.Function = structs.Function{
			FunctionType: structs.HistogramFunction,
			HistogramFunction: &structs.HistogramAgg{
				Function: sutils.HistogramQuantile,
				Quantile: quantileLiteral.Val,
			},
		}
	default:
		return fmt.Errorf("handleCallExprVectorSelectorNode: unsupported function type %v", function)
	}

	return nil
}

func handleVectorSelector(mQueryReqs []*structs.MetricsQueryRequest, intervalSeconds uint32) ([]*structs.MetricsQueryRequest, error) {
	mQuery := &mQueryReqs[0].MetricsQuery
	mQuery.HashedMName = xxhash.Sum64String(mQuery.MetricName)

	// Use the innermost aggregator of the query as the aggregator for the downsampler
	agg := structs.Aggregation{AggregatorFunction: sutils.Avg}
	if mQuery.SubsequentAggs != nil && mQuery.SubsequentAggs.AggregatorBlock != nil {
		agg.AggregatorFunction = mQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction
		agg.FuncConstant = mQuery.SubsequentAggs.AggregatorBlock.FuncConstant
		agg.GroupByFields = mQuery.SubsequentAggs.AggregatorBlock.GroupByFields
	}

	mQuery.Downsampler = structs.Downsampler{Interval: int(intervalSeconds), Unit: "s", Aggregator: agg}

	if !mQuery.SelectAllSeries {
		mQuery.SelectAllSeries = true

		// If the query has a group by (metricname or tags) or has an aggregation function, with tags,
		// then we do not need to search all the series. We can search only the series that match the tags.
		if (mQuery.AggWithoutGroupBy || mQuery.GroupByMetricName) && len(mQuery.TagsFilters) > 0 {
			mQuery.SelectAllSeries = false
		} else {
			for _, tag := range mQuery.TagsFilters {
				if tag.IsGroupByKey && !tag.NotInitialGroup {
					mQuery.SelectAllSeries = false
					break
				}
			}
		}

		// For the queries without group by and without an aggregation function, we need to get all the labels
		if mQuery.SelectAllSeries && !mQuery.Groupby && !mQuery.AggWithoutGroupBy {
			mQuery.GetAllLabels = true
		}
	}

	return mQueryReqs, nil
}

func handleBinaryExpr(expr *parser.BinaryExpr, mQueryReqs []*structs.MetricsQueryRequest,
	queryArithmetic []*structs.QueryArithmetic) ([]*structs.MetricsQueryRequest, []*structs.QueryArithmetic, error) {

	var err error

	myid := mQueryReqs[0].MetricsQuery.OrgId
	timeRange := mQueryReqs[0].TimeRange

	binaryOperation := structs.QueryArithmetic{}
	var lhsRequest, rhsRequest []*structs.MetricsQueryRequest
	var lhsQueryArth, rhsQueryArth []*structs.QueryArithmetic
	lhsIsVector := false
	rhsIsVector := false

	if len(mQueryReqs) > 0 && mQueryReqs[0].MetricsQuery.SubsequentAggs != nil {
		// This means there is a common group by on multiple metrics separated by operators.
		// So, we need to append the aggregations to the current BinaryOperation, so that
		// once the binary operation is done, we can apply the aggregations on the result.
		// And we can discard the current aggregation in the mQueryReqs[0]
		appendMetricAggsToTheMQuery(&binaryOperation, &mQueryReqs[0].MetricsQuery)
	}

	if constant, ok := expr.LHS.(*parser.NumberLiteral); ok {
		binaryOperation.ConstantOp = true
		binaryOperation.Constant = constant.Val
	} else {
		lhsRequest, _, lhsQueryArth, err = parsePromQLQuery(expr.LHS.String(), timeRange.StartEpochSec, timeRange.EndEpochSec, myid)
		if err != nil {
			return mQueryReqs, queryArithmetic, err
		}
		if len(lhsRequest) > 0 {
			lhsIsVector = true
			binaryOperation.LHS = lhsRequest[0].MetricsQuery.QueryHash
		}
		if len(lhsQueryArth) > 0 {
			binaryOperation.LHSExpr = lhsQueryArth[0]
		}
	}

	if constant, ok := expr.RHS.(*parser.NumberLiteral); ok {
		if binaryOperation.ConstantOp {
			// This implies that both LHS and RHS are constants.
			binaryOperation.RHSExpr = &structs.QueryArithmetic{
				ConstantOp: true,
				Constant:   constant.Val,
			}
		} else {
			binaryOperation.ConstantOp = true
			binaryOperation.Constant = constant.Val
		}
	} else {
		rhsRequest, _, rhsQueryArth, err = parsePromQLQuery(expr.RHS.String(), timeRange.StartEpochSec, timeRange.EndEpochSec, myid)
		if err != nil {
			return mQueryReqs, queryArithmetic, err
		}
		if len(rhsRequest) > 0 {
			binaryOperation.RHS = rhsRequest[0].MetricsQuery.QueryHash
			rhsIsVector = true
		}
		if len(rhsQueryArth) > 0 {
			binaryOperation.RHSExpr = rhsQueryArth[0]
		}
	}
	binaryOperation.Operation = putils.GetLogicalAndArithmeticOperation(expr.Op)
	binaryOperation.ReturnBool = expr.ReturnBool
	queryArithmetic = append(queryArithmetic, &binaryOperation)

	if mQueryReqs[0].MetricsQuery.HashedMName != 0 || mQueryReqs[0].MetricsQuery.SubsequentAggs != nil {
		// This means that the current MQueryReqs is not empty and we need to append the new request
		mQueryReqs = append(mQueryReqs, lhsRequest...)
	} else {
		mQueryReqs = lhsRequest
	}

	mQueryReqs = append(mQueryReqs, rhsRequest...)

	if expr.VectorMatching != nil && len(expr.VectorMatching.MatchingLabels) > 0 {
		// TODO: Fix for Logical operators. The Logical Operators can also have vector matching on labels.

		binaryOperation.VectorMatching = &structs.VectorMatching{
			Cardinality:    structs.VectorMatchCardinality(expr.VectorMatching.Card),
			MatchingLabels: expr.VectorMatching.MatchingLabels,
			On:             expr.VectorMatching.On,
		}
		sort.Strings(binaryOperation.VectorMatching.MatchingLabels)

		for i := 0; i < len(mQueryReqs); i++ {
			if len(mQueryReqs[i].MetricsQuery.TagsFilters) > 0 {
				mQueryReqs[i].MetricsQuery.SelectAllSeries = true
			}
		}
	}

	// Mathematical operations between two vectors occur when their label sets match, so it is necessary to retrieve all label sets from the vectors.
	// Logical operations also require checking whether the label sets between the vectors match
	if putils.IsLogicalOperator(binaryOperation.Operation) || (lhsIsVector && rhsIsVector) {
		for i := 0; i < len(mQueryReqs); i++ {
			mQueryReqs[i].MetricsQuery.GetAllLabels = true
		}
	}

	return mQueryReqs, queryArithmetic, nil
}

func appendMetricAggsToTheMQuery(binaryOperation *structs.QueryArithmetic, mQuery *structs.MetricsQuery) {
	binaryOperation.MQueryAggsChain = mQuery.SubsequentAggs
	mQuery.SubsequentAggs = nil
	mQuery.Function = structs.Function{}
	mQuery.FirstAggregator = structs.Aggregation{}

}

func updateMetricQueryWithAggs(mQuery *structs.MetricsQuery, mQueryAgg *structs.MetricQueryAgg) {

	// If MQueryAggs is nil, set it to the new MetricQueryAgg
	if mQuery.SubsequentAggs == nil {
		mQuery.SubsequentAggs = mQueryAgg
	} else {
		// Otherwise, set the new MetricQueryAgg to the head of the chain
		// and set the current head as the next of the new MetricQueryAgg
		mQueryAgg.Next = mQuery.SubsequentAggs
		mQuery.SubsequentAggs = mQueryAgg
	}

	// Reset the Function And Aggregator fields to handle the next function call correctly
	mQuery.Function = structs.Function{}
	mQuery.FirstAggregator = structs.Aggregation{}
}

func getStepValueFromTimeRange(timeRange *dtu.MetricsTimeRange) float64 {
	step := float64(timeRange.EndEpochSec-timeRange.StartEpochSec) / structs.MAX_POINTS_TO_EVALUATE
	return math.Max(step, 1)
}
