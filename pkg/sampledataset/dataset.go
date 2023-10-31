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

package sampledataset

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/siglens/siglens/pkg/config"
)

type Generator interface {
	Init() error
	GetLogLine() ([]byte, error)
	GetRawLog() (map[string]interface{}, error)
}

type DynamicUserGenerator struct {
	baseBody  map[string]interface{}
	tNowEpoch uint64
	tsKey     string
	faker     *gofakeit.Faker
	seed      int64
}

func InitDynamicUserGenerator(ts bool, seed int64) *DynamicUserGenerator {
	return &DynamicUserGenerator{
		seed:  seed,
		tsKey: config.GetTimeStampKey(),
	}
}

func (r *DynamicUserGenerator) Init() error {
	gofakeit.Seed(r.seed)
	r.faker = gofakeit.NewUnlocked(r.seed)
	rand.Seed(r.seed)
	r.baseBody = make(map[string]interface{})
	r.generateRandomBody()
	_, err := json.Marshal(r.baseBody)
	if err != nil {
		return err
	}
	r.tNowEpoch = uint64(time.Now().UnixMilli()) - 80*24*3600*1000
	return nil
}

func (r *DynamicUserGenerator) GetLogLine() ([]byte, error) {
	r.generateRandomBody()
	return json.Marshal(r.baseBody)
}

func (r *DynamicUserGenerator) GetRawLog() (map[string]interface{}, error) {
	r.generateRandomBody()
	return r.baseBody, nil
}

func (r *DynamicUserGenerator) generateRandomBody() {
	randomizeBody(r.faker, r.baseBody, r.tsKey)
}

func randomizeBody(f *gofakeit.Faker, m map[string]interface{}, tsKey string) {

	m["batch"] = fmt.Sprintf("batch-%d", f.Number(1, 1000))
	p := f.Person()
	m["first_name"] = p.FirstName
	m["last_name"] = p.LastName
	m["gender"] = p.Gender
	m["ssn"] = p.SSN
	m["image"] = p.Image
	m["hobby"] = p.Hobby

	m["job_description"] = p.Job.Descriptor
	m["job_level"] = p.Job.Level
	m["job_title"] = p.Job.Title
	m["job_company"] = p.Job.Company

	m["address"] = p.Address.Address
	m["street"] = p.Address.Street
	m["city"] = p.Address.City
	m["state"] = p.Address.State
	m["zip"] = p.Address.Zip
	m["country"] = p.Address.Country
	m["latitude"] = p.Address.Latitude
	m["longitude"] = p.Address.Longitude
	m["user_phone"] = p.Contact.Phone
	m["user_email"] = p.Contact.Email

	m["user_color"] = f.Color()
	m["weekday"] = f.WeekDay()
	m["http_method"] = f.HTTPMethod()
	m["http_status"] = f.HTTPStatusCodeSimple()
	m["app_name"] = f.AppName()
	m["app_version"] = f.AppVersion()
	m["ident"] = f.UUID()
	m["user_agent"] = f.UserAgent()
	m["url"] = f.URL()
	m["group"] = fmt.Sprintf("group %d", f.Number(0, 2))
	m["question"] = f.Question()
	m["latency"] = f.Number(0, 10_000_000)
	m[tsKey] = uint64(time.Now().UnixMilli())
}
