import XCTest
@testable import DataKitten

class DataKittenTests: XCTestCase {
    func testExample() throws {
        let storage = try StorageEngine(path: "/Users/robbert/Desktop/DataKittenDB")
        let db = Database(storage: storage)
        let collection = db["testcol"]
        try collection.insert(["hello": "world"])
    }


    static var allTests : [(String, (DataKittenTests) -> () throws -> Void)] {
        return [
            ("testExample", testExample),
        ]
    }
}
