// Copyright © 2016 Casa Platform
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

/*
func TestNew(t *testing.T) {
	db, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Stop()

	var message = &aabode.Message{
		Topic:   "test/testing/testy",
		Payload: []byte("just testing"),
	}
	err = db.Put(message)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := db.Get("test/testing/testy")
	if err != nil {
		t.Fatal(err)
	}

	if msg.Topic != "test/testing/testy" {
		t.Fail()
	}

	if string(msg.Payload) != "just testing" {
		t.Fail()
	}

	err = db.Stop()
	if err != nil {
		t.Fatal(err)
	}

}
*/
