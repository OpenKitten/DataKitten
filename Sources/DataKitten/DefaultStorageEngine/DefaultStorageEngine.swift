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

public enum DefaultStorageError : Error {
    case directoryDoesNotExist(atPath: String)
    case invalidHeaderDocument(key: String)
    case corruptDLTPosition(UInt64)
}

public class StorageEngine {
    let filePath: String
    fileprivate var m: Document = [:]
    
    internal var headerDocument: Document {
        get {
            return m
        }
        set {
            m = newValue
        }
    }
    
    private let lock = NSLock()
    fileprivate var headerPosition: UInt64 = 0
    private var fileHandle: FileHandle
    
    public init(path: String) throws {
        // TODO: Ensure trailing slash.
        self.filePath = path
        
        let fm = FileManager.default
        if !fm.fileExists(atPath: filePath) {
            fm.createFile(atPath: filePath, contents: nil, attributes: nil)
            self.fileHandle = try FileHandle(forUpdating: URL(fileURLWithPath: self.filePath))
            
            self.fileHandle.write(Data(bytes: [0x4d, 0x65, 0x6f, 0x77, 0x00, 13, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]))
            
            self.m = [
                "free": .binary(subtype: .generic, data: []),
                "cols": [:]
            ]
            self.headerPosition = self.fileHandle.seekToEndOfFile()
            self.fileHandle.write(Data(bytes: self.m.bytes))
        } else {
            self.fileHandle = try FileHandle(forUpdating: URL(fileURLWithPath: self.filePath))
            self.headerPosition = try self.readHeaderPosition()
            self.m = try readHeader()
        }
    }
    
    deinit {
        self.fileHandle.closeFile()
    }
    
    fileprivate func readHeaderPosition() throws -> UInt64 {
        self.fileHandle.seek(toFileOffset: 5)
        return try fromBytes(self.fileHandle.readData(ofLength: 8)) as UInt64
    }
    
    fileprivate func readHeader() throws -> Document {
        self.fileHandle.seek(toFileOffset: self.headerPosition)
        let length = try fromBytes(self.fileHandle.readData(ofLength: 4)) as UInt32
        let data = self.fileHandle.readData(ofLength: Int(length - 4))
        
        return Document(data: Data(bytes: length.bytes) + data)
    }
    
    fileprivate func writeHeader() throws -> UInt64 {
        self.fileHandle.seek(toFileOffset: self.headerPosition)
        let documentLength = try fromBytes(self.fileHandle.readData(ofLength: 4)) as Int32
        try self.removeData(atPosition: self.headerPosition, withLength: UInt32(documentLength))
        
        let position = try storeData(Data(bytes: self.headerDocument.bytes))
        self.fileHandle.seek(toFileOffset: 5)
        self.fileHandle.write(Data(bytes: position.bytes))
        
        return position
    }
    
    fileprivate func findFreeSpaces() throws -> [(location: UInt64, length: UInt32, freeSpaceLocationInheader: Int)] {
        guard case .binary(_, var free) = self.headerDocument["free"], free.count % 12 == 0 else {
            throw DefaultStorageError.invalidHeaderDocument(key: "free")
        }
        
        var freeSpaces = [(location: UInt64, length: UInt32, freeSpaceLocationInheader: Int)]()
        var position = 0
        
        while free.count >= 12 {
            freeSpaces.append((try fromBytes(free[0..<8]), try fromBytes(free[8..<12]), position))
            
            free.removeFirst(12)
            position += 12
        }
        
        return freeSpaces
    }
    
    public func removeData(atPosition startPosition: UInt64, withLength length: UInt32) throws {
        guard case .binary(_, var free) = self.headerDocument["free"] else {
            throw DefaultStorageError.invalidHeaderDocument(key: "free")
        }
        
        free.append(contentsOf: startPosition.bytes)
        free.append(contentsOf: length.bytes)
        
        self.headerDocument["free"] = .binary(subtype: .generic, data: free)
    }
    
    public func storeData(_ data: Data) throws -> UInt64 {
        self.lock.lock()
        
        defer {
            self.lock.unlock()
        }
        
        let freeSpaces = try findFreeSpaces()
        guard case .binary(_, var free) = self.headerDocument["free"] else {
            throw DefaultStorageError.invalidHeaderDocument(key: "free")
        }
        
        for space in freeSpaces where Int(space.length) >= data.count {
            self.fileHandle.seek(toFileOffset: space.location)
            self.fileHandle.write(data)
            
            let locationRange = space.freeSpaceLocationInheader..<space.freeSpaceLocationInheader + 12
            
            if data.count == Int(space.length) {
                free.removeSubrange(locationRange)
            } else {
                let newOffset = space.location + UInt64(data.count)
                let newLength = space.length - UInt32(data.count)
                
                free.replaceSubrange(locationRange, with: newOffset.bytes + newLength.bytes)
            }
            
            self.headerDocument["free"] = .binary(subtype: .generic, data: free)
            
            return space.location
        }
        
        let location = self.fileHandle.seekToEndOfFile()
        self.fileHandle.write(data)
        
        return location
    }
    
    public func storeDocument(_ document: Document, inCollectionNamed collectionName: String) throws {
        var collectionDocument: Document
        
        if let c = self.headerDocument["cols"][collectionName].documentValue {
            collectionDocument = c
        } else {
            collectionDocument = [
                "dlts": .binary(subtype: .generic, data: [])
            ]
            self.headerDocument["cols"][collectionName] = ~collectionDocument
        }
        
        guard case .binary(_, var dlts) = collectionDocument["dlts"] else {
            throw DefaultStorageError.invalidHeaderDocument(key: "dlts")
        }
        
        var dltLocations = [(UInt64, UInt32, Int)]()
        var position = 0
        
        while dlts.count >= 12 {
            let start = try fromBytes(dlts[0..<8]) as UInt64
            let length = try fromBytes(dlts[8..<12]) as UInt32
            
            dltLocations.append((start, length, position))
            dlts.removeFirst(12)
            position += 12
        }
        
        let documentLocationCollections: [(UInt64, UInt32, Data, Int)] = dltLocations.flatMap { location, length, dltPositionInArray in
            fileHandle.seek(toFileOffset: location)
            let collection = fileHandle.readData(ofLength: Int(length))
            guard collection.count == Int(length) else {
                return nil
            }
            
            return (location, length, collection, dltPositionInArray)
        }
        
        let documentPosition = try storeData(Data(bytes: document.bytes))
        let documentLocationData = documentPosition.bytes + UInt32(document.byteCount).bytes
        
        let maxDLTDocuments = 10
        
        defer {
            do {
                self.headerDocument["cols"][collectionName] = ~collectionDocument
                self.headerPosition = try writeHeader()
            } catch {}
        }
        
        for (location, length, documentLocations, dltPositionInArray) in documentLocationCollections where documentLocations.count < maxDLTDocuments * 12 {
            var documentLocations = documentLocations
            documentLocations.append(contentsOf: documentLocationData)
            try self.removeData(atPosition: location, withLength: length)
            let dltLocation = try self.storeData(documentLocations)
            
            guard case .binary(_, var dltLocations) = collectionDocument["dlts"] else {
                throw DefaultStorageError.invalidHeaderDocument(key: "dlts")
            }
            
            dltLocations.replaceSubrange(dltPositionInArray..<dltPositionInArray + 12, with: dltLocation.bytes + UInt32(documentLocations.count).bytes)
            
            collectionDocument["dlts"] = .binary(subtype: .generic, data: dltLocations)
            
            return
        }
        
        if case .binary(_, var dlts) = collectionDocument["dlts"] {
            let dltLocation = try storeData(Data(bytes: documentLocationData))
            dlts.append(contentsOf: dltLocation.bytes + documentLocationData.count.bytes)
            collectionDocument["dlts"] = .binary(subtype: .generic, data: dlts)
            
            return
        }
        
        throw DefaultStorageError.invalidHeaderDocument(key: "dlts")
    }
    
    public func makeDataIterator(inCollectionNamed collection: String) throws -> AnyIterator<Data> {
        guard case .binary(_, var dlts) = self.headerDocument["cols"][collection]["dlts"] else {
            return AnyIterator { return nil }
        }
        
        var dltLocations = [(UInt64, UInt32, Int)]()
        var position = 0
        
        while dlts.count >= 12 {
            let start = try fromBytes(dlts[0..<8]) as UInt64
            let length = try fromBytes(dlts[8..<12]) as UInt32
            
            dltLocations.append((start, length, position))
            dlts.removeFirst(12)
            position += 12
        }
        
        let documentLocationTables: [(UInt64, UInt32, Data, Int)] = dltLocations.flatMap { location, length, dltPositionInArray in
            fileHandle.seek(toFileOffset: location)
            let collection = fileHandle.readData(ofLength: Int(length))
            guard collection.count == Int(length) else {
                return nil
            }
            
            return (location, length, collection, dltPositionInArray)
        }
        
        var documentLocations = [(UInt64, UInt32)]()
        
        for DLT in documentLocationTables {
            fileHandle.seek(toFileOffset: DLT.0)
            var data = fileHandle.readData(ofLength: Int(DLT.1))
            
            guard data.count == Int(DLT.1) && data.count % 12 == 0 else {
                throw DefaultStorageError.corruptDLTPosition(DLT.0)
            }
            
            while data.count >= 12 {
                documentLocations.append((try fromBytes(data[0..<8]), try fromBytes(data[8..<12])))
                data.removeFirst(12)
            }
        }
        
        var iterator = documentLocations.makeIterator()
        
        return AnyIterator {
            guard let dataMetadata = iterator.next() else {
                return nil
            }
            
            self.fileHandle.seek(toFileOffset: dataMetadata.0)
            return self.fileHandle.readData(ofLength: Int(dataMetadata.1))
        }
    }
}
