import PackageDescription

let package = Package(
    name: "DataKitten",
    dependencies: [
        .Package(url: "https://github.com/OpenKitten/BSON.git", Version(0,0,12345))
    ]
)
