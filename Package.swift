import PackageDescription

let package = Package(
	name: "PostgreSQL",
	dependencies: [
		.Package(url: "https://github.com/Zewo/CLibpq.git", majorVersion: 0, minor: 5),
		.Package(url: "https://github.com/mjmsmith/SQL.git", majorVersion: 0, minor: 0),
	]
)
