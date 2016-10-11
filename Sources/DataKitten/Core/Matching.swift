import BSON

extension Document {
    public func matches(_ filter: Document) -> Bool {
        for (field, value) in filter {
            let ownValue = self[field]
            
            if case .int32(let i) = value, case .int64(let i2) = ownValue, Int64(i) != i2 {
                return false
            }
            
            if case .int32(let i) = ownValue, case .int64(let i2) = value, Int64(i) != i2 {
                return false
            }
            
            guard ownValue == value else {
                return false
            }
        }
        
        return true
    }
}
