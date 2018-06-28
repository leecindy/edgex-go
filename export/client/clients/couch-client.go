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
	"fmt"
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

	exists, err := client.DBExists(context.TODO(), "test")
	if err != nil {
		panic(err)
	}

	if !exists {
		err := client.CreateDB(context.TODO(), "test")
		if err != nil {
			panic(err)
		}
	}

	db, err := client.DB(context.TODO(), "test")
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
	cc.Database.Put(context.TODO(), id.Hex(), reg)
	return id, nil
}

func (cc *CouchClient) UpdateRegistration(reg export.Registration) error{
	fmt.Println("Calling update")//delete later
	fmt.Println("ID ", reg.ID.Hex())//delete later
	fmt.Println("name", reg.Name)//delete later
	//fmt.Println("rev", reg.Rev)//delete later

	_, err := cc.Database.Put(context.TODO(), reg.ID.Hex(), reg)
	if err != nil {
		panic(err)
	}
	//reg.Rev = newRev
	return  nil
}

func (cc *CouchClient) RegistrationById(id string) (export.Registration, error){
	fmt.Println("RegistrationById", id) //delete later
	var reg export.Registration
	r, err := cc.Database.Get(context.TODO(), id)
	if err != nil {
		panic(err)
	}

	err = r.ScanDoc(&reg);
	if err != nil {
		panic(err)
	}

	fmt.Println("2After Get ", reg.ID.Hex())//delete later
	fmt.Println("2After Get ", reg.Name)//delete later
	fmt.Println("2After Get ", reg.Enable)//delete later
	//fmt.Println("2After Get ", reg.Rev)//delete later
	return reg, err
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

func (cc *CouchClient) CloseSession() {

}

