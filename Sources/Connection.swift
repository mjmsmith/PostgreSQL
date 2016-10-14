// Connection.swift
//
// The MIT License (MIT)
//
// Copyright (c) 2015 Formbound
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

import Foundation
@_exported import SQL
import CLibpq

public struct ConnectionError: Error, CustomStringConvertible {
  public let description: String
}

public final class Connection: ConnectionProtocol {
  public static let willExecuteSQL = Notification.Name("PostgreSQL.Connection.willExecuteSQL") // object = "<sql>"
  
  public struct ConnectionInfo: ConnectionInfoProtocol {
    public var host: String
    public var port: Int
    public var databaseName: String
    public var username: String?
    public var password: String?
    public var options: String?
    public var tty: String?
        
    public init?(uri: URL) {
      do {
        try self.init(uri)
      }
      catch {
        return nil
      }
    }

    public init(_ uri: URL) throws {
      let databaseName = uri.path.trimmingCharacters(in: ["/"])
          
      guard let host = uri.host, let port = uri.port else {
        throw ConnectionError(description: "Failed to extract host, port, database name from URI")
      }
          
      self.host = host
      self.port = port
      self.databaseName = databaseName
      self.username = uri.user
      self.password = uri.password
    }

    public init(host: String, port: Int = 5432, databaseName: String, password: String? = nil, options: String? = nil, tty: String? = nil) {
      self.host = host
      self.port = port
      self.databaseName = databaseName
      self.password = password
      self.options = options
      self.tty = tty
    }
  }
  
  public enum InternalStatus {
    case Bad
    case Started
    case Made
    case AwatingResponse
    case AuthOK
    case SettingEnvironment
    case SSLStartup
    case OK
    case Unknown
    case Needed
        
    public init(status: ConnStatusType) {
      switch status {
        case CONNECTION_NEEDED:
          self = .Needed
        case CONNECTION_OK:
          self = .OK
        case CONNECTION_STARTED:
          self = .Started
        case CONNECTION_MADE:
          self = .Made
        case CONNECTION_AWAITING_RESPONSE:
          self = .AwatingResponse
        case CONNECTION_AUTH_OK:
          self = .AuthOK
        case CONNECTION_SSL_STARTUP:
          self = .SSLStartup
        case CONNECTION_SETENV:
          self = .SettingEnvironment
        case CONNECTION_BAD:
          self = .Bad
        default:
          self = .Unknown
      }
    }
  }
  
  private var connection: OpaquePointer? = nil

  public let connectionInfo: ConnectionInfo

  public required init(info: ConnectionInfo) {
    self.connectionInfo = info
  }
    
  deinit {
    close()
  }
    
  public var internalStatus: InternalStatus {
    return InternalStatus(status: PQstatus(self.connection))
  }
    
  public func open() throws {
    connection = PQsetdbLogin(
      connectionInfo.host,
      String(connectionInfo.port),
      connectionInfo.options ?? "",
      connectionInfo.tty ?? "",
      connectionInfo.databaseName,
      connectionInfo.username ?? "",
      connectionInfo.password ?? ""
    )
        
    if let error = mostRecentError {
      throw error
    }
  }
    
  public var mostRecentError: ConnectionError? {
    guard let errorString = String(validatingUTF8: PQerrorMessage(connection)) , !errorString.isEmpty else {
      return nil
    }
        
    return ConnectionError(description: errorString)
  }
    
  public func close() {
    PQfinish(connection)
    connection = nil
  }
    
  public func createSavePointNamed(_ name: String) throws {
    let _ = try execute("SAVEPOINT \(name)")
  }
    
  public func rollbackToSavePointNamed(_ name: String) throws {
    let _ = try execute("ROLLBACK TO SAVEPOINT \(name)")
  }
    
  public func releaseSavePointNamed(_ name: String) throws {
    let _ = try execute("RELEASE SAVEPOINT \(name)")
  }
  
  @discardableResult
  public func executeInsertQuery<T: SQLDataConvertible>(query: InsertQuery, returningPrimaryKeyForField primaryKey: DeclaredField) throws -> T {

    var components = query.queryComponents
    components.append(QueryComponents(strings: ["RETURNING", primaryKey.qualifiedName, "AS", "returned__pk"]))

    DispatchQueue.global(qos: .background).async() {
      NotificationCenter.default.post(name: Connection.willExecuteSQL, object: components.string, userInfo: nil)
    }
    
    let result = try execute(components)
        
    guard let pk: T = try result.first?.value("returned__pk") else {
      throw ConnectionError(description: "Did not receive returned primary key")
    }
        
    return pk
  }
    
  @discardableResult
  public func execute(_ components: QueryComponents) throws -> Result {
    DispatchQueue.global(qos: .background).async() {
      NotificationCenter.default.post(name: Connection.willExecuteSQL, object: components.string, userInfo: nil)
    }
      
    guard !components.values.isEmpty else {
      guard let resultPointer = PQexec(connection, components.string) else {
        throw mostRecentError ?? ConnectionError(description: "Empty result")
      }
          
      return try Result(resultPointer)
    }

    var parameterData: [UnsafePointer<Int8>?] = []
    var deallocators = [() -> ()]()
    defer { deallocators.forEach { $0() } }

    for parameter in components.values {
      guard let value = parameter else {
        parameterData.append(nil)
        continue
      }
        
      let data: AnyCollection<Int8>
        
      switch value {
        case .Binary(let value):
          data = AnyCollection(value.map { Int8($0) })
        case .Text(let string):
          data = AnyCollection(string.utf8CString)
      }
              
      let pointer = UnsafeMutablePointer<Int8>.allocate(capacity: Int(data.count))
  
      deallocators.append {
        pointer.deallocate(capacity: Int(data.count))
      }
              
      for (index, byte) in data.enumerated() {
        pointer[index] = byte
      }
          
      parameterData.append(pointer)
    }

    let result: OpaquePointer = try parameterData.withUnsafeBufferPointer { buffer in
      guard let result = PQexecParams(
        self.connection,
        try components.stringWithEscapedValuesUsingPrefix("$") { index, _ in
          return String(index + 1)
        },
        Int32(components.values.count),
        nil,
        buffer.isEmpty ? nil : buffer.baseAddress,
        nil,
        nil,
        0
      ) else {
        throw mostRecentError ?? ConnectionError(description: "Empty result")
      }
        
      return result
    }
      
    return try Result(result)
  }
}
