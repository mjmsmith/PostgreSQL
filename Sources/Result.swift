// Result.swift
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
import CLibpq
@_exported import SQL

public class Result: SQL.ResultProtocol {
    
  public enum ResultError: Error {
    case BadStatus(Status, String)
  }
    
  public enum Status: Int, ResultStatus {
    case EmptyQuery
    case CommandOK
    case TuplesOK
    case CopyOut
    case CopyIn
    case BadResponse
    case NonFatalError
    case FatalError
    case CopyBoth
    case SingleTuple
    case Unknown
        
    public init(status: ExecStatusType) {
      switch status {
        case PGRES_EMPTY_QUERY:
          self = .EmptyQuery
        case PGRES_COMMAND_OK:
          self = .CommandOK
        case PGRES_TUPLES_OK:
          self = .TuplesOK
        case PGRES_COPY_OUT:
          self = .CopyOut
        case PGRES_COPY_IN:
          self = .CopyIn
        case PGRES_BAD_RESPONSE:
          self = .BadResponse
        case PGRES_NONFATAL_ERROR:
          self = .NonFatalError
        case PGRES_FATAL_ERROR:
          self = .FatalError
        case PGRES_COPY_BOTH:
          self = .CopyBoth
        case PGRES_SINGLE_TUPLE:
          self = .SingleTuple
        default:
          self = .Unknown
      }
    }
        
    public var successful: Bool {
      return self != .BadResponse && self != .FatalError
    }
  }

  internal init(_ resultPointer: OpaquePointer) throws {
    self.resultPointer = resultPointer
        
    guard status.successful else {
      throw ResultError.BadStatus(status, String(validatingUTF8: PQresultErrorMessage(resultPointer)) ?? "No error message")
    }
  }
    
  deinit {
    clear()
  }
    
  public subscript(position: Int) -> Row {
    let index = Int32(position)
    var result: [String: Data?] = [:]
        
    for (fieldIndex, field) in fields.enumerated() {
      let fieldIndex = Int32(fieldIndex)
            
      if PQgetisnull(resultPointer, index, fieldIndex) == 1 {
        result[field.name] = nil
      }
      else {
        result[field.name] = Data(
          pointer: PQgetvalue(resultPointer, index, fieldIndex),
          length: Int(PQgetlength(resultPointer, index, fieldIndex))
        )
      }
    }
        
    return Row(dataByfield: result)
  }
    
  public var count: Int {
      return Int(PQntuples(self.resultPointer))
  }
    
  lazy public var countAffected: Int = {
    guard let str = String(validatingUTF8: PQcmdTuples(self.resultPointer)) else {
      return 0
    }
        
    return Int(str) ?? 0
  }()
    
  public var status: Status {
    return Status(status: PQresultStatus(resultPointer))
  }
    
  private let resultPointer: OpaquePointer
    
  public func clear() {
    PQclear(resultPointer)
  }
    
  public lazy var fields: [FieldInfo] = {
    var result: [FieldInfo] = []
        
    for i in 0..<PQnfields(self.resultPointer) {
      guard let fieldName = String(validatingUTF8: PQfname(self.resultPointer, i)) else {
        continue
      }
            
      result.append(FieldInfo(name: fieldName))
    }
        
    return result
  }()
}

extension Data {
  public init(pointer: UnsafePointer<Int8>, length: Int) {
    self = Data(bytes: pointer, count: length)
  }
}
