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

	"github.com/brianvoe/gofakeit/v6"
	log "github.com/sirupsen/logrus"
)

var dynamicUserColumnNames = []string{
	"batch",
	"first_name",
	"last_name",
	"gender",
	"ssn",
	"image",
	"hobby",
	"job_description",
	"job_level",
	"job_title",
	"job_company",
	"address",
	"street",
	"city",
	"state",
	"zip",
	"country",
	"latitude",
	"longitude",
	"user_phone",
	"user_email",
	"user_color",
	"weekday",
	"http_method",
	"http_status",
	"app_name",
	"app_version",
	"ident",
	"user_agent",
	"url",
	"group",
	"question",
	"latency",
}

func getDynamicUserColumnValue(f *gofakeit.Faker, columnName string, p *gofakeit.PersonInfo) interface{} {
	switch columnName {
	case "batch":
		return fmt.Sprintf("batch-%d", f.Number(1, 1000)) // Corrected the batch value format
	case "first_name":
		return p.FirstName
	case "last_name":
		return p.LastName
	case "gender":
		return p.Gender
	case "ssn":
		return p.SSN
	case "image":
		return p.Image
	case "hobby":
		return p.Hobby
	case "job_description":
		return p.Job.Descriptor
	case "job_level":
		return p.Job.Level
	case "job_title":
		return p.Job.Title
	case "job_company":
		return p.Job.Company
	case "address":
		return p.Address.Address
	case "street":
		return p.Address.Street
	case "city":
		return p.Address.City
	case "state":
		return p.Address.State
	case "zip":
		return p.Address.Zip
	case "country":
		return p.Address.Country
	case "latitude":
		return p.Address.Latitude
	case "longitude":
		return p.Address.Longitude
	case "user_phone":
		return p.Contact.Phone
	case "user_email":
		return p.Contact.Email
	case "user_color":
		return f.Color()
	case "weekday":
		return f.WeekDay()
	case "http_method":
		return f.HTTPMethod()
	case "http_status":
		return f.HTTPStatusCodeSimple()
	case "app_name":
		return f.AppName()
	case "app_version":
		return f.AppVersion()
	case "ident":
		return f.UUID()
	case "user_agent":
		return f.UserAgent()
	case "url":
		return f.URL()
	case "group":
		return fmt.Sprintf("group %d", f.Number(0, 2))
	case "question":
		return f.Question()
	case "latency":
		return f.Number(0, 10_000_000)
	}

	log.Errorf("getDynamicUserColumnValue: unknown column name %s", columnName)
	log.Infof("getDynamicUserColumnValue: returning BuzzWord")
	return f.BuzzWord()
}
