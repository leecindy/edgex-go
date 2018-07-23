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
	"github.com/edgexfoundry/edgex-go/core/domain/models"
	"fmt"
	"time"
)

type CouchClient struct {
	Database *kivik.DB
}

type CouchRegistration struct {
	ID          bson.ObjectId      			`bson:"_id,omitempty" json:"_id,omitempty"`
	Rev			string			   			`json:"_rev,omitempty" json:"rev,omitempty"` //required for update
	Created     int64              			`json:"created"`
	Modified    int64              			`json:"modified"`
	Origin      int64              			`json:"origin"`
	Name        string             			`json:"name,omitempty"`
	Addressable models.Addressable 			`json:"addressable,omitempty"`
	Format      string             			`json:"format,omitempty"`
	Filter      export.Filter      			`json:"filter,omitempty"`
	Encryption  export.EncryptionDetails	`json:"encryption,omitempty"`
	Compression string           		    `json:"compression,omitempty"`
	Enable      bool              		    `json:"enable"`
	Destination string            		    `json:"destination,omitempty"`
}

// Return a pointer to the MongoClient
func newCouchClient(config DBConfiguration) (*CouchClient, error) {
	// Create the dial info for the Mongo session
	connectionString := "http://" + config.Host + ":" + strconv.Itoa(config.Port)

	ctx1, cancel1 := context.WithTimeout(context.Background(),(3 * time.Second))
	defer cancel1()
	client, err := kivik.New(ctx1, "couch", connectionString)


	//usersDB, _ := client.DB(context.TODO(), "_users") // Connect to the _users database
	//user := map[string]interface{}{
	//	"_id":      kivik.UserPrefix + config.Username,
	//	"type":     "user",
	//	"password": config.Password,
	//}
	//
	//usersDBExists, err := client.DBExists(context.TODO(), "_users")
	//if err !=nil {
	//	panic(err)
	//}
	//fmt.Println("usersDBExists", usersDBExists)
	//
	//if !usersDBExists {
	//	err := client.CreateDB(context.TODO(), "_users")
	//	if err != nil {
	//		panic(err)
	//	}
	//}
	//
	//usersDB.Put(context.TODO(), kivik.UserPrefix+config.Username, user)

	ctx2, cancel2 := context.WithTimeout(context.Background(),(3 * time.Second))
	defer cancel2()
	clientExists, err := client.DBExists(ctx2, "test")
	if err != nil {
		panic(err)
	}

	if !clientExists {
		ctx3, cancel3 := context.WithTimeout(context.Background(),(3 * time.Second))
		defer cancel3()
		err := client.CreateDB(ctx3, "test")
		if err != nil {
			panic(err)
		}
	}

	db, err := client.DB(context.TODO(), "test")
	if err != nil {
		panic(err)
	}
	//
	//db.Put(context.TODO(), "_design/example", map[string]interface{}{
	//	"_id": "_design/example",
	//	"views": map[string]interface{}{
	//		"new-view": map[string]interface{}{
	//			"map": "function(doc){emit(doc._id, 1); emit(doc.name, 2);}",
	//		},
	//	},
	//})


	return &CouchClient{Database:db}, nil
}

func (cc *CouchClient) Registrations() ([]export.Registration, error) {
	var regs []export.Registration
	rows, err := cc.Database.AllDocs(context.TODO(), kivik.Options{"include_docs":true})
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var reg export.Registration
		err = rows.ScanDoc(&reg)
		if err != nil {
			return nil, err
		}
		reg.ID = bson.ObjectIdHex(rows.ID())
		regs = append(regs, reg)
	}

	return regs, err
}

func (cc *CouchClient) AddRegistration(reg *export.Registration) (bson.ObjectId, error){
	id := bson.NewObjectId()

	cc.Database.Put(context.TODO(), id.Hex(), reg)
	reg.ID = id
	return reg.ID, nil
}

func ConvertToCouchReg(reg export.Registration) CouchRegistration{
	var couchReg CouchRegistration
	//couchReg.ID = reg.ID
	couchReg.Created = reg.Created
	couchReg.Modified = reg.Modified
	couchReg.Origin = reg.Origin
	couchReg.Name = reg.Name
	couchReg.Addressable = reg.Addressable
	couchReg.Format = reg.Format
	couchReg.Filter = reg.Filter
	couchReg.Encryption = reg.Encryption
	couchReg.Compression = reg.Compression
	couchReg.Enable = reg.Enable
	couchReg.Destination = reg.Destination
	return couchReg
}

func ConvertToReg(testReg CouchRegistration) export.Registration{
	var reg export.Registration
	//couchReg.ID = reg.ID
	reg.Created = testReg.Created
	reg.Modified = testReg.Modified
	reg.Origin = testReg.Origin
	reg.Name = testReg.Name
	reg.Addressable = testReg.Addressable
	reg.Format = testReg.Format
	reg.Filter = testReg.Filter
	reg.Encryption = testReg.Encryption
	reg.Compression = testReg.Compression
	reg.Enable = testReg.Enable
	reg.Destination = testReg.Destination
	return reg
}


func (cc *CouchClient) UpdateRegistration(reg export.Registration) error{
	rev, err := cc.Database.Rev(context.TODO(), reg.ID.Hex())
	if err != nil {
		panic(err)
	}

	couchReg := ConvertToCouchReg(reg)
	couchReg.Rev = rev

	cc.Database.Put(context.TODO(), reg.ID.Hex(), couchReg)

	return  nil
}

func (cc *CouchClient) RegistrationById(id string) (export.Registration, error){
	var reg export.Registration
	row, err := cc.Database.Get(context.TODO(), id)

	if err != nil {
		panic(err)
	}

	err = row.ScanDoc(&reg);
	if err != nil {
		panic(err)
	}
	return reg, err
}

func (cc *CouchClient) RegistrationByName(name string) (export.Registration, error){
	var reg export.Registration
	var newReg CouchRegistration

	findName := map[string]interface{}{"selector": map[string]interface{}{"name": map[string]interface{}{"$eq": name}}}
	rows, err := cc.Database.Find(context.TODO(), findName)

	if err != nil {
		panic(err)
	}
	for rows.Next() {
		err = rows.ScanDoc(&newReg)
		fmt.Println(newReg)
		if err != nil {
			panic(err)
		}
	}
	reg = ConvertToReg(newReg)
	reg.ID = newReg.ID

	if reg.ID.Hex() == "" {
		return export.Registration{}, ErrNotFound
	}

	return reg, err


}

func (cc *CouchClient) DeleteRegistrationById(id string) error {
	_, err := cc.RegistrationById(id)
	if err!= nil {
		panic(err)
	}

	rev, err := cc.Database.Rev(context.TODO(), id)
	cc.Database.Delete(context.TODO(), id, rev)
	return err
}

func (cc *CouchClient) DeleteRegistrationByName(name string) error {
	var reg export.Registration

	reg, _ = cc.RegistrationByName(name)

	return cc.DeleteRegistrationById(reg.ID.Hex())
}

func (cc *CouchClient) CloseSession() {

}

