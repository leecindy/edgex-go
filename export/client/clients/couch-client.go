/*******************************************************************************
 * Copyright 1995-2018 Hitachi Vantara Corporation. All rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 *******************************************************************************/
package clients

import (
	"strconv"
	"github.com/flimzy/kivik"
	"context"
	_ "github.com/go-kivik/couchdb"
	"github.com/edgexfoundry/edgex-go/export"
	"gopkg.in/mgo.v2/bson"
)

type CouchClient struct {
	Database *kivik.DB
}

// Return a pointer to the MongoClient
func newCouchClient(config DBConfiguration) (*CouchClient, error) {
	// Create the dial info for the Mongo session
	connectionString := "http://" + config.Host + ":" + strconv.Itoa(config.Port)
	client, err := kivik.New(context.TODO(), "couch", connectionString)
	if err != nil {
		panic(err)
	}

	exists, err := client.DBExists(context.TODO(), "cindy")
	if err != nil {
		panic(err)
	}
	if !exists {
		err := client.CreateDB(context.TODO(), "cindy")
		if err != nil {
			panic(err)
		}
	}

	db, err := client.DB(context.TODO(), "cindy")
	if err != nil {
		panic(err)
	}

	return &CouchClient{Database:db}, nil
}

func (cc *CouchClient) Registrations() ([]export.Registration, error) {
	return nil, nil
}

func (cc *CouchClient) AddRegistration(reg *export.Registration) (bson.ObjectId, error){
	id := bson.NewObjectId()
	cc.Database.Put(context.TODO(), id.Hex(), reg )
	return id, nil
}

func (cc *CouchClient) UpdateRegistration(reg export.Registration) error{
	return nil
}

func (cc *CouchClient) RegistrationById(id string) (export.Registration, error){
	return export.Registration{}, nil
}

func (cc *CouchClient) RegistrationByName(name string) (export.Registration, error){
	return export.Registration{}, ErrNotFound
}

func (cc *CouchClient) DeleteRegistrationById(id string) error {
	return nil
}

func (cc *CouchClient) DeleteRegistrationByName(name string) error {
	return nil
}

func (cc *CouchClient) getRegistrations(q bson.M) ([]export.Registration, error) {
	return nil, nil
}

func (cc *CouchClient) getRegistration(q bson.M) (export.Registration, error) {
	return export.Registration{}, nil
}

func (cc *CouchClient) deleteRegistration(q bson.M) error {
	return nil
}

func (cc *CouchClient) CloseSession() {

}

