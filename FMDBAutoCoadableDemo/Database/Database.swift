//
//  Database.swift
//  FMDBAutoCoadableDemo
//
//  Created by mac on 2024/10/23.
//

// 初版，后续都是在这基础上改的

//import FMDB
//
//enum DBColumnType {
//    case text
//    case integer
//    case real
//    case blob
//    
//    var sqlType: String {
//        switch self {
//        case .text: return "TEXT"
//        case .integer: return "INTEGER"
//        case .real: return "REAL"
//        case .blob: return "BLOB"
//        }
//    }
//}
//
//// 数据库字段信息
//struct DBColumnInfo {
//    let name: String
//    let type: DBColumnType
//    let isPrimaryKey: Bool
//}
//
//// 数据库操作错误枚举
//enum DatabaseError: Error {
//    case encodingFailed
//    case decodingFailed
//    case invalidType
//    case tableCreationFailed
//    case insertionFailed
//}
//
//// 数据库表协议
//protocol DatabaseTable: Codable {
//    static var tableName: String { get }
//    static func primaryKey() -> String
//}
//
//// Database Manager类
//class DatabaseManager {
//    private let db: FMDatabase
//    
//    static func getDatabasePath() -> String {
//        let documentDirectory = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true).first!
//        return (documentDirectory as NSString).appendingPathComponent("app.db")
//    }
//    
//    init(path: String) {
//        printl(message: DatabaseManager.getDatabasePath())
//        db = FMDatabase(path: DatabaseManager.getDatabasePath())
//        db.open()
//    }
//    
//    deinit {
//        db.close()
//    }
//    
//    // 创建表
//    func createTable<T: DatabaseTable>(_ object: T) throws {
//        let mirror = Mirror(reflecting: object)
//        var columns: [DBColumnInfo] = []
//        
//        // 解析模型属性
//        for child in mirror.children {
//            guard let label = child.label else { continue }
//            
//            let columnType: DBColumnType
//            let isPrimaryKey = label == T.primaryKey()
//            
//            let valueType = type(of: child.value)
//            
//            switch valueType {
//            case is String.Type, is Optional<String>.Type:
//                columnType = .text
//            case is Int.Type, is Int32.Type, is Int64.Type,
//                is Optional<Int>.Type, is Optional<Int32>.Type, is Optional<Int64>.Type:
//                columnType = .integer
//            case is Double.Type, is Float.Type,
//                is Optional<Double>.Type, is Optional<Float>.Type:
//                columnType = .real
//            case is Codable.Type, is Optional<Codable>.Type:
//                columnType = .blob
//            default:
//                throw DatabaseError.invalidType
//            }
//            
//            columns.append(DBColumnInfo(name: label, type: columnType, isPrimaryKey: isPrimaryKey))
//        }
//        
//        // 生成建表SQL
//        let createSQL = generateCreateTableSQL(tableName: T.tableName, columns: columns)
//        
//        if !db.executeUpdate(createSQL, withArgumentsIn: []) {
//            throw DatabaseError.tableCreationFailed
//        }
//    }
//    
//    // 插入数据
//    func insert<T: DatabaseTable>(_ object: T) throws {
//        let mirror = Mirror(reflecting: object)
//        var columns: [String] = []
//        var values: [Any] = []
//        
//        // 处理属性值
//        for child in mirror.children {
//            guard let label = child.label else { continue }
//            columns.append(label)
//            
//            let valueType = type(of: child.value)
//            
//            switch valueType {
//            case is String.Type, is Optional<String>.Type
//                , is Int.Type, is Int32.Type, is Int64.Type,
//                is Optional<Int>.Type, is Optional<Int32>.Type, is Optional<Int64>.Type
//                , is Double.Type, is Float.Type,
//                is Optional<Double>.Type, is Optional<Float>.Type:
//                values.append(child.value)
//            case is Codable.Type, is Optional<Codable>.Type:
//                if let codableValue = child.value as? Codable {
//                    do {
//                        let data = try JSONEncoder().encode(codableValue)
//                        values.append(data)
//                    } catch {
//                        throw DatabaseError.encodingFailed
//                    }
//                }
//            default:
//                throw DatabaseError.invalidType
//            }
//        }
//        
//        // 生成插入SQL
//        let insertSQL = generateInsertSQL(tableName: T.tableName, columns: columns)
//        
//        if !db.executeUpdate(insertSQL, withArgumentsIn: values) {
//            throw DatabaseError.insertionFailed
//        }
//    }
//    
//    // 查询数据
//    func query<T: DatabaseTable>(_ objectType: T, where condition: String? = nil) throws -> [T] {
//        var sql = "SELECT * FROM \(T.tableName)"
//        if let condition = condition {
//            sql += " WHERE \(condition)"
//        }
//        
//        guard let resultSet = db.executeQuery(sql, withArgumentsIn: []) else {
//            return []
//        }
//        
//        var results: [T] = []
//        
//        while resultSet.next() {
//            var dictionary: [String: Any] = [:]
//            let mirror = Mirror(reflecting: objectType)
//            
//            for child in mirror.children {
//                guard let label = child.label else { continue }
//                
//                let valueType = type(of: child.value)
//                
//                switch valueType {
//                case is String.Type, is Optional<String>.Type
//                    , is Int.Type, is Int32.Type, is Int64.Type,
//                    is Optional<Int>.Type, is Optional<Int32>.Type, is Optional<Int64>.Type
//                    , is Double.Type, is Float.Type,
//                    is Optional<Double>.Type, is Optional<Float>.Type:
//                    dictionary[label] = resultSet.object(forColumn: label)
//                case is Codable.Type, is Optional<Codable>.Type:
//                    if let blobData = resultSet.data(forColumn: label) {
//                        
//                        dictionary[label] = try blobData.jsonObject()
//                        
//                        //                        let typeName = String(describing: valueType)
//                        //                            let cleanTypeName = typeName
//                        //                                .replacingOccurrences(of: "Optional<", with: "")
//                        //                                .replacingOccurrences(of: ">", with: "")
//                        //                                .split(separator: ".")
//                        //                                .last.map(String.init) ?? typeName // 获取最后一个部分
//                        //
//                        //                        printl(message: cleanTypeName)
//                        //
//                        //                        let namespace = Bundle.main.infoDictionary!["CFBundleExecutable"] as! String
//                        //                        let cls = NSClassFromString(namespace + "." + cleanTypeName)! as! Decodable.Type
//                        //
//                        //                        let decoder = JSONDecoder()
//                        //                        do {
//                        //                            let decodedValue = try decoder.decode(cls, from: blobData)
//                        ////                            dictionary[label] = String(data: blobData, encoding: .utf8)
//                        //
//                        //                        } catch {
//                        //                            throw DatabaseError.decodingFailed
//                        //                        }
//                    }
//                default:
//                    throw DatabaseError.invalidType
//                }
//            }
//            
//            // 将字典转换为模型对象
//            let jsonData = try JSONSerialization.data(withJSONObject: dictionary)
//            let object = try JSONDecoder().decode(T.self, from: jsonData)
//            results.append(object)
//        }
//        
//        return results
//    }
//    
//    // 生成建表SQL语句
//    private func generateCreateTableSQL(tableName: String, columns: [DBColumnInfo]) -> String {
//        var sql = "CREATE TABLE IF NOT EXISTS \(tableName) ("
//        
//        let columnDefinitions = columns.map { column in
//            var def = "\(column.name) \(column.type.sqlType)"
//            if column.isPrimaryKey {
//                def += " PRIMARY KEY"
//            }
//            return def
//        }
//        
//        sql += columnDefinitions.joined(separator: ", ")
//        sql += ")"
//        
//        return sql
//    }
//    
//    // 生成插入SQL语句
//    private func generateInsertSQL(tableName: String, columns: [String]) -> String {
//        let columnString = columns.joined(separator: ", ")
//        let valuePlaceholders = Array(repeating: "?", count: columns.count).joined(separator: ", ")
//        
//        return "INSERT OR REPLACE INTO \(tableName) (\(columnString)) VALUES (\(valuePlaceholders))"
//    }
//}
//
//
//// MARK: - data扩展
//extension Data {
//    func jsonObject(options opt: JSONSerialization.ReadingOptions = []) throws -> Any? {
//        return try? JSONSerialization.jsonObject(with: self, options: opt)
//    }
//}


/// 判断是否枚举
//if let blobData = dic[label] as? Data {
//                        if T.enumDic.keys.contains(label) {
//                            let type = T.enumDic[label]!
//                            let enumValue = try JSONDecoder().decode(type, from: blobData)
//                            if let rawRepresentableValue = enumValue as? (any RawRepresentable),
//                                       let intValue = rawRepresentableValue.rawValue as? String {
//                                        print("枚举的原始值是: \(intValue)")  // 输出枚举对应的 Int 值
//                                dictionary[label] = intValue  // 将整数值存入 enumDictionary
//                                    } else {
//                                        print("解码后的值不是遵循 RawRepresentable 的枚举，或 RawValue 不是 Int")
//                                    }
//                        } else {
//                            dictionary[label] = try blobData.jsonObject()
//                        }
//                    }
