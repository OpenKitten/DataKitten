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

import BSON
import Foundation

public class Collection {
    let db: Database
    let name: String
    
    internal init(named name: String, in database: Database) {
        self.name = name
        self.db = database
    }
    
    @discardableResult
    public func insert(_ document: Document) throws -> Value {
        var document = document
        var id = document["_id"]
        if id == .nothing || id == .null {
            id = ~ObjectId()
            document["_id"] = id
        }
        
        try db.storageEngine.storeDocument(Data(bytes: document.bytes), inCollectionNamed: name)
        
        return id
    }
    
    public func findOne() throws -> Document? {
        if let data = try self.db.storageEngine.findDocuments(inCollectionNamed: name).next() {
            return Document(data: data)
        }
        
        return nil
    }
    
    public func find() throws -> [Document] {
        return try self.db.storageEngine.findDocuments(inCollectionNamed: name).makeIterator().flatMap { data in
            return Document(data: data)
        }
    }
}
