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
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func TestJson(t *testing.T) {
	cases := []struct {
		input string
		want  map[string]interface{}
	}{
		{ // case#1
			`{
			   "key1": "val1",
			   "key2": 1.456,
			   "key3": true,
			   "key4": "John"
			 }`,
			map[string]interface{}{
				"key1": "val1",
				"key2": 1.456,
				"key3": true,
				"key4": "John",
			},
		},
		{ //case 2
			`{
			   "foo": {
				   "jim":"bean"
				},
			   "fee": "bar"
			 }`,
			map[string]interface{}{
				"foo.jim": "bean",
				"fee":     "bar",
			},
		},
		{ // case 3
			`{ "a": { "b" : { "c" : { "d" : "e" } } }, "number": 1.4567, "bool": true }`,
			map[string]interface{}{
				"a.b.c.d": "e",
				"number":  1.4567,
				"bool":    true,
			},
		},
		{ // case 4
			`{ "a": ["v1", "v2"], "b": [0.0,1.1,2.2], "bool": true }`,
			map[string]interface{}{
				"a.0":  "v1",
				"a.1":  "v2",
				"b.0":  0.0,
				"b.1":  1.1,
				"b.2":  2.2,
				"bool": true,
			},
		},
	}

	for i, test := range cases {
		var m map[string]interface{}
		err := json.Unmarshal([]byte(test.input), &m)
		if err != nil {
			t.Errorf("testid: %d: Failed to parse json err:%v", i+1, err)
			continue
		}

		got := Flatten(m)

		//		t.Logf("got: %v", got)
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("case: %d: mismatch: got: [%v], wanted: [%v]", i+1, got, test.want)
		}
	}

}

func Benchmark_flatten(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	input := `{ "a": { "b" : { "c" : { "d" : "e" } } }, "number": 1.4567, "bool": true }`

	var m map[string]interface{}
	err := json.Unmarshal([]byte(input), &m)
	if err != nil {
		b.Errorf("Failed to parse json err:%v", err)
	}

	for i := 0; i < b.N; i++ {
		Flatten(m)
	}
}
func unmarshalString(t *testing.T, data string) map[string]interface{} {
	var m map[string]interface{}
	err := json.Unmarshal([]byte(data), &m)
	if err != nil {
		return nil
	}
	fmt.Println(m)
	return m
}

func TestFlatten(t *testing.T) {
	data1 := `{"responseElements": {"gOEZw": {"CToqk": "vynlmwGIgk", "VfWbM": "lGuFJDkDip"}, "AjMej": {"vGKXa": "uIzuwIpvfg", "iIVEs": "hgssPSNtMD"}, "rjAON": [{"NECnv": "KuwTanjNIz", "frnTG": "yoVLglyswf"}, {"Lgzrq": "MKpSdGRjCk", "ZkYEt": "ogbPVfdWCp"}, {"cmUmd": {"fxtub": "LKXvMmqDPn", "lpElF": "TWcFwOfntu"}, "GADuH": {"GloWr": "wrMUHqSLnP", "aWRAk": "HbkHXlfiZF"}, "LBiSQ": {"lcaYz": "uAwADPJonD", "EHdTD": "vRTEyiylDO"}, "eoJsl": [{"nyCIH": "AQdoctvPin", "GGBIc": "VFXotQcQaY"}]}]}}`
	data2 := `{"eventTime": "2022-03-31T04:04:24Z", "eventVersion": "1.2", "userIdentity": {"type": "IAMUser", "principalId": "A9YRGIZJ7LTYX7E6ZBR9", "arn": "arn:aws:iam::327525440610:user/Tina_Long", "accountId": "327525440610", "accessKeyId": "BFWANGMQK15TDRHEDMDD", "userName": "Tina_Long"}, "eventSource": "ebs.amazonaws.com", "eventName": "SignOut", "awsRegion": "us-west-2", "sourceIPAddress": "80.99.17.33", "userAgent": "aws-sdk-ruby", "errorCode": "Often.", "errorMessage": "Most peace material.", "requestParameters": {"JftbB": {"Eptxk": "qPrPxRooyD"}, "xuqKB": {"FHcUk": "rYYfGwtBqR"}, "FBQGY": {"ncDnH": "LYBLkKkaZg"}, "naqJE": {"ISzCV": "HCugGPfDuG"}, "rcXQZ": []}, "responseElements": {"gOEZw": {"CToqk": "vynlmwGIgk", "VfWbM": "lGuFJDkDip"}, "AjMej": {"vGKXa": "uIzuwIpvfg", "iIVEs": "hgssPSNtMD"}, "rjAON": [{"NECnv": "KuwTanjNIz", "frnTG": "yoVLglyswf"}, {"Lgzrq": "MKpSdGRjCk", "ZkYEt": "ogbPVfdWCp"}]}, "requestID": "0d82f6e4-a7ca-4b76-a542-a4a5182490f2", "eventID": "6ca824c8-81c4-41d3-9de4-58ab902d1158", "eventType": "AwsConsoleSignIn", "apiVersion": "6.1", "managementEvent": "AwsConsoleAction", "resources": [{"ARN": "arn:aws:iam::647294929174:role/SXkLH", "accountId": "647294929174", "type": "AWS::s3::SXkLH"}, {"ARN": "arn:aws:iam::935171735452:role/Auqhr", "accountId": "935171735452", "type": "AWS::lambda::Auqhr"}, {"ARN": "arn:aws:iam::914027419284:role/gXxts", "accountId": "914027419284", "type": "AWS::ebs::gXxts"}], "recipientAccountId": "446069363933", "sharedEventID": "f434d6a1-22f2-4bea-89b6-0505d9e30b5d", "vpcEndpointId": "vpc-18328331", "eventCategory": "Insight"}`
	data3 := `{"eventTime": "2022-03-31T04:04:24Z", "eventVersion": "1.2", "userIdentity": {"type": "IAMUser", "principalId": "A9YRGIZJ7LTYX7E6ZBR9", "arn": "arn:aws:iam::327525440610:user/Tina_Long", "accountId": "327525440610", "accessKeyId": "BFWANGMQK15TDRHEDMDD", "userName": "Tina_Long"}, "eventSource": "ebs.amazonaws.com", "eventName": "SignOut", "awsRegion": "us-west-2", "sourceIPAddress": "80.99.17.33", "userAgent": "aws-sdk-ruby", "errorCode": "Often.", "errorMessage": "Most peace material.", "requestParameters": {"JftbB": {"Eptxk": "qPrPxRooyD"}, "xuqKB": {"FHcUk": "rYYfGwtBqR"}, "FBQGY": {"ncDnH": "LYBLkKkaZg"}, "naqJE": {"ISzCV": "HCugGPfDuG"}, "rcXQZ": []}, "responseElements": {"gOEZw": {"CToqk": "vynlmwGIgk", "VfWbM": "lGuFJDkDip"}, "AjMej": {"vGKXa": "uIzuwIpvfg", "iIVEs": "hgssPSNtMD"}, "rjAON": [{"NECnv": "KuwTanjNIz", "frnTG": "yoVLglyswf"}, {"Lgzrq": "MKpSdGRjCk", "ZkYEt": "ogbPVfdWCp"}, {"cmUmd": {"fxtub": "LKXvMmqDPn", "lpElF": "TWcFwOfntu"}, "GADuH": {"GloWr": "wrMUHqSLnP", "aWRAk": "HbkHXlfiZF"}, "LBiSQ": {"lcaYz": "uAwADPJonD", "EHdTD": "vRTEyiylDO"}, "eoJsl": [{"nyCIH": "AQdoctvPin", "GGBIc": "VFXotQcQaY"}]}]}, "requestID": "0d82f6e4-a7ca-4b76-a542-a4a5182490f2", "eventID": "6ca824c8-81c4-41d3-9de4-58ab902d1158", "eventType": "AwsConsoleSignIn", "apiVersion": "6.1", "managementEvent": "AwsConsoleAction", "resources": [{"ARN": "arn:aws:iam::647294929174:role/SXkLH", "accountId": "647294929174", "type": "AWS::s3::SXkLH"}, {"ARN": "arn:aws:iam::935171735452:role/Auqhr", "accountId": "935171735452", "type": "AWS::lambda::Auqhr"}, {"ARN": "arn:aws:iam::914027419284:role/gXxts", "accountId": "914027419284", "type": "AWS::ebs::gXxts"}], "recipientAccountId": "446069363933", "sharedEventID": "f434d6a1-22f2-4bea-89b6-0505d9e30b5d", "vpcEndpointId": "vpc-18328331", "eventCategory": "Insight"}`

	type args struct {
		m map[string]interface{}
	}
	tests := []struct {
		name string
		args args
		want map[string]interface{}
	}{
		{
			name: "Test Valid Case Data1",
			args: args{
				m: unmarshalString(t, data1),
			},
			want: map[string]interface{}{
				"responseElements.AjMej.iIVEs":           "hgssPSNtMD",
				"responseElements.AjMej.vGKXa":           "uIzuwIpvfg",
				"responseElements.gOEZw.CToqk":           "vynlmwGIgk",
				"responseElements.gOEZw.VfWbM":           "lGuFJDkDip",
				"responseElements.rjAON.0.NECnv":         "KuwTanjNIz",
				"responseElements.rjAON.0.frnTG":         "yoVLglyswf",
				"responseElements.rjAON.1.Lgzrq":         "MKpSdGRjCk",
				"responseElements.rjAON.1.ZkYEt":         "ogbPVfdWCp",
				"responseElements.rjAON.2.GADuH.GloWr":   "wrMUHqSLnP",
				"responseElements.rjAON.2.GADuH.aWRAk":   "HbkHXlfiZF",
				"responseElements.rjAON.2.LBiSQ.EHdTD":   "vRTEyiylDO",
				"responseElements.rjAON.2.LBiSQ.lcaYz":   "uAwADPJonD",
				"responseElements.rjAON.2.cmUmd.fxtub":   "LKXvMmqDPn",
				"responseElements.rjAON.2.cmUmd.lpElF":   "TWcFwOfntu",
				"responseElements.rjAON.2.eoJsl.0.GGBIc": "VFXotQcQaY",
				"responseElements.rjAON.2.eoJsl.0.nyCIH": "AQdoctvPin",
			},
		},
		{
			name: "Test Valid Case Data2",
			args: args{
				m: unmarshalString(t, data2),
			},
			want: map[string]interface{}{
				"apiVersion":                     "6.1",
				"awsRegion":                      "us-west-2",
				"errorCode":                      "Often.",
				"errorMessage":                   "Most peace material.",
				"eventCategory":                  "Insight",
				"eventID":                        "6ca824c8-81c4-41d3-9de4-58ab902d1158",
				"eventName":                      "SignOut",
				"eventSource":                    "ebs.amazonaws.com",
				"eventTime":                      "2022-03-31T04:04:24Z",
				"eventType":                      "AwsConsoleSignIn",
				"eventVersion":                   "1.2",
				"managementEvent":                "AwsConsoleAction",
				"recipientAccountId":             "446069363933",
				"requestID":                      "0d82f6e4-a7ca-4b76-a542-a4a5182490f2",
				"requestParameters.FBQGY.ncDnH":  "LYBLkKkaZg",
				"requestParameters.JftbB.Eptxk":  "qPrPxRooyD",
				"requestParameters.naqJE.ISzCV":  "HCugGPfDuG",
				"requestParameters.xuqKB.FHcUk":  "rYYfGwtBqR",
				"resources.0.ARN":                "arn:aws:iam::647294929174:role/SXkLH",
				"resources.0.accountId":          "647294929174",
				"resources.0.type":               "AWS::s3::SXkLH",
				"resources.1.ARN":                "arn:aws:iam::935171735452:role/Auqhr",
				"resources.1.accountId":          "935171735452",
				"resources.1.type":               "AWS::lambda::Auqhr",
				"resources.2.ARN":                "arn:aws:iam::914027419284:role/gXxts",
				"resources.2.accountId":          "914027419284",
				"resources.2.type":               "AWS::ebs::gXxts",
				"responseElements.AjMej.iIVEs":   "hgssPSNtMD",
				"responseElements.AjMej.vGKXa":   "uIzuwIpvfg",
				"responseElements.gOEZw.CToqk":   "vynlmwGIgk",
				"responseElements.gOEZw.VfWbM":   "lGuFJDkDip",
				"responseElements.rjAON.0.NECnv": "KuwTanjNIz",
				"responseElements.rjAON.0.frnTG": "yoVLglyswf",
				"responseElements.rjAON.1.Lgzrq": "MKpSdGRjCk",
				"responseElements.rjAON.1.ZkYEt": "ogbPVfdWCp",
				"sharedEventID":                  "f434d6a1-22f2-4bea-89b6-0505d9e30b5d",
				"sourceIPAddress":                "80.99.17.33",
				"userAgent":                      "aws-sdk-ruby",
				"userIdentity.accessKeyId":       "BFWANGMQK15TDRHEDMDD",
				"userIdentity.accountId":         "327525440610",
				"userIdentity.arn":               "arn:aws:iam::327525440610:user/Tina_Long",
				"userIdentity.principalId":       "A9YRGIZJ7LTYX7E6ZBR9",
				"userIdentity.type":              "IAMUser",
				"userIdentity.userName":          "Tina_Long",
				"vpcEndpointId":                  "vpc-18328331",
			},
		},
		{
			name: "Test Valid Case Data3",
			args: args{
				m: unmarshalString(t, data3),
			},
			want: map[string]interface{}{
				"apiVersion":                             "6.1",
				"awsRegion":                              "us-west-2",
				"errorCode":                              "Often.",
				"errorMessage":                           "Most peace material.",
				"eventCategory":                          "Insight",
				"eventID":                                "6ca824c8-81c4-41d3-9de4-58ab902d1158",
				"eventName":                              "SignOut",
				"eventSource":                            "ebs.amazonaws.com",
				"eventTime":                              "2022-03-31T04:04:24Z",
				"eventType":                              "AwsConsoleSignIn",
				"eventVersion":                           "1.2",
				"managementEvent":                        "AwsConsoleAction",
				"recipientAccountId":                     "446069363933",
				"requestID":                              "0d82f6e4-a7ca-4b76-a542-a4a5182490f2",
				"requestParameters.FBQGY.ncDnH":          "LYBLkKkaZg",
				"requestParameters.JftbB.Eptxk":          "qPrPxRooyD",
				"requestParameters.naqJE.ISzCV":          "HCugGPfDuG",
				"requestParameters.xuqKB.FHcUk":          "rYYfGwtBqR",
				"resources.0.ARN":                        "arn:aws:iam::647294929174:role/SXkLH",
				"resources.0.accountId":                  "647294929174",
				"resources.0.type":                       "AWS::s3::SXkLH",
				"resources.1.ARN":                        "arn:aws:iam::935171735452:role/Auqhr",
				"resources.1.accountId":                  "935171735452",
				"resources.1.type":                       "AWS::lambda::Auqhr",
				"resources.2.ARN":                        "arn:aws:iam::914027419284:role/gXxts",
				"resources.2.accountId":                  "914027419284",
				"resources.2.type":                       "AWS::ebs::gXxts",
				"responseElements.AjMej.iIVEs":           "hgssPSNtMD",
				"responseElements.AjMej.vGKXa":           "uIzuwIpvfg",
				"responseElements.gOEZw.CToqk":           "vynlmwGIgk",
				"responseElements.gOEZw.VfWbM":           "lGuFJDkDip",
				"responseElements.rjAON.0.NECnv":         "KuwTanjNIz",
				"responseElements.rjAON.0.frnTG":         "yoVLglyswf",
				"responseElements.rjAON.1.Lgzrq":         "MKpSdGRjCk",
				"responseElements.rjAON.1.ZkYEt":         "ogbPVfdWCp",
				"responseElements.rjAON.2.GADuH.GloWr":   "wrMUHqSLnP",
				"responseElements.rjAON.2.GADuH.aWRAk":   "HbkHXlfiZF",
				"responseElements.rjAON.2.LBiSQ.EHdTD":   "vRTEyiylDO",
				"responseElements.rjAON.2.LBiSQ.lcaYz":   "uAwADPJonD",
				"responseElements.rjAON.2.cmUmd.fxtub":   "LKXvMmqDPn",
				"responseElements.rjAON.2.cmUmd.lpElF":   "TWcFwOfntu",
				"responseElements.rjAON.2.eoJsl.0.GGBIc": "VFXotQcQaY",
				"responseElements.rjAON.2.eoJsl.0.nyCIH": "AQdoctvPin",
				"sharedEventID":                          "f434d6a1-22f2-4bea-89b6-0505d9e30b5d",
				"sourceIPAddress":                        "80.99.17.33",
				"userAgent":                              "aws-sdk-ruby",
				"userIdentity.accessKeyId":               "BFWANGMQK15TDRHEDMDD",
				"userIdentity.accountId":                 "327525440610",
				"userIdentity.arn":                       "arn:aws:iam::327525440610:user/Tina_Long",
				"userIdentity.principalId":               "A9YRGIZJ7LTYX7E6ZBR9",
				"userIdentity.type":                      "IAMUser",
				"userIdentity.userName":                  "Tina_Long",
				"vpcEndpointId":                          "vpc-18328331",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Flatten(tt.args.m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Flatten() = %v, want %v", got, tt.want)
			}
		})
	}
}
