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


import Foundation

public enum DefaultStorageError : Error {
    case directoryDoesNotExist(atPath: String)
}

public class StorageEngine {
    let basePath: String
    
    public init(path: String) throws {
        var isDirectory: ObjCBool = false
        guard FileManager.default.fileExists(atPath: path, isDirectory: &isDirectory) else {
            throw DefaultStorageError.directoryDoesNotExist(atPath: path)
        }
        
        // TODO: Ensure trailing slash.
        self.basePath = path + "/"
    }
    
    private var fileHandles = [String: FileHandle]()
    private func getHandle(forCollectionNamed collection: String) throws -> FileHandle {
        if let handle = fileHandles[collection] {
            return handle
        }
        
        let filePath = basePath + collection + ".collection"
        
        // Create the file if it doesn't exist yet
        let fm = FileManager.default
        if !fm.fileExists(atPath: filePath) {
            fm.createFile(atPath: filePath, contents: nil, attributes: nil)
        }
        
        let url = URL(fileURLWithPath: filePath)
        let handle = try FileHandle(forUpdating: url)
        fileHandles[collection] = handle
        
        return handle
    }
    
    public func storeDocument(_ document: Document, inCollectionNamed collection: String) throws {
        let handle = try getHandle(forCollectionNamed: collection)
        handle.seekToEndOfFile()
        let data = Data(bytes: document.bytes)
        handle.write(data)
    }
    
    public func findDocuments(inCollectionNamed collection: String) throws -> DefaultCursor {
        let handle = try getHandle(forCollectionNamed: collection)
        return DefaultCursor(handle: handle)
    }
}


