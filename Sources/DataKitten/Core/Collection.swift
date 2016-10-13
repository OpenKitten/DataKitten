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
        
        try db.storageEngine.storeDocument(document, inCollectionNamed: name)
        
        return id
    }
    
    @discardableResult
    public func insert(_ documents: [Document]) throws -> [Value] {
        guard documents.count > 0 else {
            return []
        }
        
        var documents = documents.map { doc -> (Document) in
            var doc = doc
            
            if doc["_id"] == .nothing {
                doc["_id"] = ~ObjectId()
            }
            
            return doc
        }
        
        let lastDocument = documents.removeLast()
        
        for document in documents {
            try db.storageEngine.storeDocument(document, inCollectionNamed: self.name, writingHeaders: false)
        }
        
        try db.storageEngine.storeDocument(lastDocument, inCollectionNamed: self.name, writingHeaders: true)
        
        return documents.map{ $0["_id"] } + [lastDocument["_id"]]
    }
    
    public func findOne(matching filter: Document? = nil) throws -> Document? {
        return try find(matching: filter).next()
    }
    
    public func find(matching filter: Document? = nil) throws -> AnyIterator<Document> {
        let iter = try self.db.storageEngine.makeDataIterator(inCollectionNamed: name)
            
        return AnyIterator {
            var document: Document? = nil
            
            repeat {
                guard let data = iter.next() else {
                    return nil
                }
                
                document = Document(data: data)
                
                if (document ?? [:]).matches(filter ?? [:]) {
                    return document
                }
            } while(document != nil)
            
            return nil
        }
    }
    
    public func count(matching filter: Document? = nil) throws -> Int {
        if filter == nil {
            return try self.db.storageEngine.count(inCollection: self.name)
        }
        
        return Array(try self.find(matching: filter)).count
    }
    
    @discardableResult
    public func remove(matching filter: Document? = nil, multiple: Bool = false) throws -> Int {
        let iter = try self.db.storageEngine.makeFullDataIterator(inCollectionNamed: name)
        
        var document: Document? = nil
        var removed = 0
        
        repeat {
            guard let data = iter.next() else {
                return removed
            }
            
            document = Document(data: data.0)
            
            if (document ?? [:]).matches(filter ?? [:]) {
                try self.db.storageEngine.removeDocument(fromCollection: self.name, atPosition: data.1, withLength: UInt32(data.0.count))
                removed += 1
                
                if !multiple {
                    return removed
                }
            }
        } while(document != nil)
        
        return removed
    }
}
