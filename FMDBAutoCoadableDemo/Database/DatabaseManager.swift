import FMDB

enum DBColumnType {
    case text
    case integer
    case real
    // 单独处理bool的存储
    case bool
    case blob
    
    var sqlType: String {
        switch self {
        case .text: return "TEXT"
        case .integer: return "INTEGER"
        case .real: return "REAL"
        case .bool: return "INTEGER"
        case .blob: return "BLOB"
        }
    }
}

// 数据库字段信息
struct DBColumnInfo {
    let name: String
    let type: DBColumnType
    let isPrimaryKey: Bool
}

// 数据库操作错误枚举
enum DatabaseError: Error {
    case encodingFailed
    case decodingFailed
    case invalidType
    case tableCreationFailed
    case insertionFailed
}

// 数据库表协议
protocol DatabaseTable: Codable {
    static var tableName: String { get }
    static func primaryKey() -> String
}

let dbName = "testApp.db"

// Database Manager类
class DatabaseManager {
    private let db: FMDatabase
    
    init() {
        let paths = NSSearchPathForDirectoriesInDomains(.cachesDirectory, .userDomainMask, true)
        let cachesDirectory = paths[0]
        let writableDBPath = (cachesDirectory as NSString).appendingPathComponent(dbName)
        
        printl(message: writableDBPath)
        
        db = FMDatabase(path: writableDBPath)
        
        if !db.open() {
            printl(message: "打开数据库失败: \(db.lastErrorMessage())")
        }
    }
    
    deinit {
        db.close()
    }
    
    // 创建表
    func createTable<T: DatabaseTable>(_ object: T) throws {
        if isExistTable(T.tableName) {
            printl(message: "表已存在")
            return
        }
        
        let mirror = Mirror(reflecting: object)
        var columns: [DBColumnInfo] = []
        
        // 解析模型属性
        for child in mirror.children {
            guard let label = child.label else { continue }
            
            let columnType: DBColumnType
            let isPrimaryKey = label == T.primaryKey()
            
            let valueType = type(of: child.value)
            
            switch valueType {
            case is String.Type, is Optional<String>.Type:
                columnType = .text
            case is Int.Type, is Int32.Type, is Int64.Type,
                is Optional<Int>.Type, is Optional<Int32>.Type, is Optional<Int64>.Type:
                columnType = .integer
            case is Double.Type, is Float.Type,
                is Optional<Double>.Type, is Optional<Float>.Type:
                columnType = .real
            case is Bool.Type, is Optional<Bool>.Type:
                columnType = .bool
            case is Codable.Type, is Optional<Codable>.Type:
                columnType = .blob
            default:
                throw DatabaseError.invalidType
            }
            
            columns.append(DBColumnInfo(name: label, type: columnType, isPrimaryKey: isPrimaryKey))
        }
        
        // 生成建表SQL
        let createSQL = generateCreateTableSQL(tableName: T.tableName, columns: columns)
        
        let reg = insertDataWithSQL(createSQL, values: [])
        if !reg {
            printl(message: "create \(T.tableName) failed")
            throw DatabaseError.tableCreationFailed
        }
    }
    
    // 插入数据
    func insert<T: DatabaseTable>(_ object: T) throws {
        let mirror = Mirror(reflecting: object)
        var columns: [String] = []
        var values: [Any] = []
        
        // 处理属性值
        for child in mirror.children {
            guard let label = child.label else { continue }
            columns.append(label)
            
            let valueType = type(of: child.value)
            
            switch valueType {
            case is String.Type, is Optional<String>.Type
                , is Int.Type, is Int32.Type, is Int64.Type,
                is Optional<Int>.Type, is Optional<Int32>.Type, is Optional<Int64>.Type
                , is Double.Type, is Float.Type,
                is Optional<Double>.Type, is Optional<Float>.Type:
                values.append(child.value)
            case is Bool.Type, is Optional<Bool>.Type:
                values.append((child.value as? Bool ?? false) ? 1 : 0)
            case is Codable.Type, is Optional<Codable>.Type
                :
                if let codableValue = child.value as? Codable {
                    do {
                        let data = try JSONEncoder().encode(codableValue)
                        values.append(data)
                    } catch {
                        throw DatabaseError.encodingFailed
                    }
                }
            default:
                throw DatabaseError.invalidType
            }
        }
        
        // 生成插入SQL
        let insertSQL = generateInsertSQL(tableName: T.tableName, columns: columns)
        
        let reg = insertDataWithSQL(insertSQL, values: values)
        if !reg {
            printl(message: "insert \(T.tableName) failed")
            throw DatabaseError.insertionFailed
        }
    }
    
    // 查询数据
    func query<T: DatabaseTable>(_ objectType: T, where condition: String? = nil) throws -> [T] {
        var sql = "SELECT * FROM \(T.tableName)"
        if let condition = condition {
            sql += " WHERE \(condition)"
        }
        
        var results: [T] = []
        
        let tempArray = getDataBySQL(sql, values: [])
        
        for dic in tempArray {
            var dictionary: [String: Any] = dic
            
            let mirror = Mirror(reflecting: objectType)
            
            for child in mirror.children {
                guard let label = child.label else { continue }
                
                let valueType = type(of: child.value)
                
                switch valueType {
                case is String.Type, is Optional<String>.Type
                    , is Int.Type, is Int32.Type, is Int64.Type,
                    is Optional<Int>.Type, is Optional<Int32>.Type, is Optional<Int64>.Type
                    , is Double.Type, is Float.Type,
                    is Optional<Double>.Type, is Optional<Float>.Type:
                    continue
                case is Bool.Type, is Optional<Bool>.Type:
                    dictionary[label] = dic[label] as? Bool
                case is Codable.Type, is Optional<Codable>.Type:
                    if let blobData = dic[label] as? Data {
                        dictionary[label] = try blobData.jsonObject()
                    }
                default:
                    printl(message: "不支持类型")
                    throw DatabaseError.invalidType
                }
            }
            
            // 将字典转换为模型对象
            let jsonData = try JSONSerialization.data(withJSONObject: dictionary)
            let object = try JSONDecoder().decode(T.self, from: jsonData)
            results.append(object)
        }
        
        return results
    }
    
    func deleteTable(from tableName: String, otherSqlDic sqlDic: [String: String]?) -> Bool {
        var deleteSql = "DELETE FROM \(tableName)"
        
        if let sqlDic = sqlDic, !sqlDic.isEmpty {
            deleteSql.append(" WHERE")
            
            for (key, value) in sqlDic {
                deleteSql.append(" \(key) = '\(value)'")
            }
        }
        
        print("Delete data SQL: \(deleteSql)")
        
        return deleteDataWithSQL(deleteSql, values: [])
    }

}

// MARK: - sql语句拼接，以及执行
extension DatabaseManager {
    /// 查询
    fileprivate func getDataBySQL(_ sql: String, values: [Any]) -> [[String: Any]] {
        var results: [[String: Any]] = []
        if db.open() {
            db.shouldCacheStatements = true
            guard let resultSet = db.executeQuery(sql, withArgumentsIn: values) else {
                printl(message: "未从数据库查询到数据")
                return results
            }
            if db.hadError() {
                printl(message: "error \(db.lastErrorCode()) : \(db.lastErrorMessage())")
            }
            
            while resultSet.next() {
                if let dic = resultSet.resultDictionary as? [String: Any] {
                    results.append(dic)
                }
            }
            db.close()
        }
        return results
    }
    
    // 插入
    fileprivate func insertDataWithSQL(_ sql: String, values: [Any]) -> Bool {
        var result: Bool = true
        if db.open() {
            db.shouldCacheStatements = true
            db.executeUpdate(sql, withArgumentsIn: values)
            if db.hadError() {
                printl(message: "error \(db.lastErrorCode()) : \(db.lastErrorMessage())")
                result = false
            }
            db.close()
        }
        return result
    }
    
    // 删除
    fileprivate func deleteDataWithSQL(_ sql: String, values: [Any]) -> Bool {
        var result: Bool = true
        if db.open() {
            db.shouldCacheStatements = true
            db.executeUpdate(sql, withArgumentsIn: values)
            if db.hadError() {
                printl(message: "error \(db.lastErrorCode()) : \(db.lastErrorMessage())")
                result = false
            }
            db.close()
        }
        return result
    }
    
    // 建表SQL语句
    fileprivate func generateCreateTableSQL(tableName: String, columns: [DBColumnInfo]) -> String {
        var sql = "CREATE TABLE IF NOT EXISTS \(tableName) ("
        
        let columnDefinitions = columns.map { column in
            var def = "\(column.name) \(column.type.sqlType)"
            if column.isPrimaryKey {
                def += " PRIMARY KEY"
            }
            return def
        }
        
        sql += columnDefinitions.joined(separator: ", ")
        sql += ")"
        
        return sql
    }
    
    // 插入SQL语句
    fileprivate func generateInsertSQL(tableName: String, columns: [String]) -> String {
        let columnString = columns.joined(separator: ", ")
        let valuePlaceholders = Array(repeating: "?", count: columns.count).joined(separator: ", ")
        
        return "INSERT OR REPLACE INTO \(tableName) (\(columnString)) VALUES (\(valuePlaceholders))"
    }
    
    // 删除SQL语句
    fileprivate func generateDeleteSQL(tableName: String, condition: String) -> String {
        return "DELETE FROM \(tableName) WHERE \(condition)"
    }
    
    // clear SQL语句
    fileprivate func generateClearSQL(tableName: String) -> String {
        return "DELETE FROM \(tableName)"
    }
}

// MARK: - 表相关
extension DatabaseManager {
    // 判断表是否存在
    func isExistTable(_ tableName: String) -> Bool {
        let sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='\(tableName)'"
        let arr = getDataBySQL(sql, values: [])

        guard arr.count > 0 else {
            return false
        }
        
        if let count = arr[0]["count(*)"] as? Int {
            return count > 0
        }
        
        return false
    }

    // 清理缓存 直接删除的数据库
    class func updateVersionCleanCache() {
        DispatchQueue.global(qos: .default).async {
            let fileManager = FileManager.default
            let paths = NSSearchPathForDirectoriesInDomains(.cachesDirectory, .userDomainMask, true)
            let documentsDirectory = paths[0]
            
            do {
                let fileList = try fileManager.contentsOfDirectory(atPath: documentsDirectory)
                for tempPath in fileList {
                    if tempPath.contains(dbName) {
                        let fullPath = (documentsDirectory as NSString).appendingPathComponent(tempPath)
                        do {
                            try fileManager.removeItem(atPath: fullPath)
                            print("Remove \(tempPath) Success")
                        } catch {
                            print("Error removing \(tempPath): \(error.localizedDescription)")
                        }
                    }
                }
            } catch {
                print("Error retrieving contents of directory: \(error.localizedDescription)")
            }
        }
    }

}

// MARK: - data扩展
extension Data {
    func jsonObject(options opt: JSONSerialization.ReadingOptions = []) throws -> Any? {
        return try? JSONSerialization.jsonObject(with: self, options: opt)
    }
}
