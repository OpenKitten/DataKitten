// Copyright (c) 2016 Joannis Orlandos & Robbert Brandsma
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
// associated documentation files (the "Software"), to deal in the Software without restriction,
// including without limitation the rights to use, copy, modify, merge, publish, distribute,
// sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or
// substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
// NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import XCTest
@testable import DataKitten

class DataKittenTests: XCTestCase {
    var storage: StorageEngine! = nil
    var db: Database! = nil
    var col: DataKitten.Collection! = nil
    
    override func setUp() {
        self.storage = try! StorageEngine(path: "/Users/joannis/Desktop/database.dk")
        self.db = Database(storage: storage)
        self.col = db["testcol"]
    }
    
    override func tearDown() {
        try! storage.optimizeFreeSpace()
    }
    
    func testInsert() throws {
        try col.insert(["hello": "world"])
    }
    
    func testBulkInsert() throws {
        for _ in 0..<1221 {try col.insert(["hello": "world"])}
    }
    
    func testFindOne() throws {
        try col.insert(["hello": "world"])
        
        guard let doc = try col.findOne() else {
            XCTFail("no document found")
            return
        }
        
        XCTAssertEqual(doc["hello"], "world")
    }
    
    func testFind() throws {
        let docs = Array(try col.find())
        
        XCTAssertGreaterThan(docs.count, 1)
        
        for doc in docs {
            XCTAssertEqual(doc["hello"], "world")
        }
        
        print(docs.count)
    }
    
    func testBasics() throws {
        for i in 0..<10 {
            try col.insert(["henk": ~i])
        }
        
        for _ in 0..<20 {
            try col.insert(["henk": "piet"])
        }
        
        XCTAssertEqual(Array(try col.find(matching: ["henk": "piet"])).count, 20)
        XCTAssertEqual(Array(try col.find(matching: ["henk": 3])).count, 1)
        
        var removed = try col.remove(matching: ["henk": "piet"], multiple: false)
        
        XCTAssertEqual(removed, 1)
        
        XCTAssertEqual(Array(try col.find(matching: ["henk": "piet"])).count, 19)
        XCTAssertEqual(Array(try col.find(matching: ["henk": 3])).count, 1)
        
        removed = try col.remove(matching: ["henk": "piet"], multiple: true)
        
        XCTAssertEqual(removed, 19)
        
        XCTAssertEqual(Array(try col.find(matching: ["henk": "piet"])).count, 0)
        XCTAssertEqual(Array(try col.find(matching: ["henk": 3])).count, 1)
        
        XCTAssertGreaterThanOrEqual(try col.remove(matching: [:], multiple: true), 1)
    }
}
