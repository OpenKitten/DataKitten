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
import BSON

public class DefaultCursor : Cursor {
    private let handle: FileHandle
    private var seekPosition: UInt64 = 0 // the start position of the next document to examine in a full collection scan
    
    internal init(handle: FileHandle) {
        self.handle = handle
    }
    
    public override func next() throws -> Document? {
        handle.seek(toFileOffset: seekPosition)
        let lengthData = handle.readData(ofLength: 4)
        
        if lengthData.count < 4 {
            return nil
        }
        
        let length = try fromBytes(lengthData) as Int32
        
        // TODO: Do not crash on invalid bson length specification
        let documentData = lengthData + handle.readData(ofLength: length-4)
        
        seekPosition += UInt64(length)
        
        return Document(data: documentData)
    }
}
