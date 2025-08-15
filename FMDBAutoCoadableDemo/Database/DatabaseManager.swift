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

/// 数据库存储model时包含枚举类型属性，暂时支持int string
public enum DBModelEnumType: Int {
    case `Int`
    case `String`
}

public protocol DummyInitializable {
    static func initDummyInstance() -> Self
}

extension DummyInitializable where Self: Codable {
    public static func initDummyInstance() -> Self {
        let json = "{}".data(using: .utf8)!
        return try! JSONDecoder().decode(Self.self, from: json)
    }
}

// 数据库表协议
public protocol DatabaseTable: Codable, DummyInitializable {
    static var tableName: String { get }
    static func primaryKey() -> String
    /// 自定义枚举映射
    static var enumPropertyMapper: [String: DBModelEnumType] { get }
}

public extension DatabaseTable {
    static var tableName: String {
        return ""
    }
    
    static func primaryKey() -> String {
        return ""
    }
    
    static var enumPropertyMapper: [String: DBModelEnumType] {
        return [:]
    }
}


let dbName = "testApp.db"

// Database Manager类
class DatabaseManager {
    /// fmdb transation insert tuple
    typealias InsertTransactionTuple = (sql: String, values: [Any])
    
    private let dbQueue: FMDatabaseQueue
    
    private let dbVersionKey = "DBVersion"
    let newDBVersion = "2.0"
    
    init(userId: String? = nil) {
        let cachesDirectory = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first!
        
        var targetDbDirectoryName: String
        if let userId = userId, !userId.isEmpty {
            targetDbDirectoryName = userId + "DB"
        } else {
            targetDbDirectoryName = "DB"
        }
        let dbDirectory = cachesDirectory.appendingPathComponent(targetDbDirectoryName)
        let writableDBPath = dbDirectory.appendingPathComponent(dbName).path
        
        if !FileManager.default.fileExists(atPath: writableDBPath) {
            do {
                try FileManager.default.createDirectory(at: dbDirectory, withIntermediateDirectories: true, attributes: nil)
            } catch {
                printl(message: "创建db文件夹失败: \(error.localizedDescription)")
            }
        }
        
        printl(message: writableDBPath)
        
        if let db = FMDatabaseQueue(path: writableDBPath) {
            self.dbQueue = db
        } else {
            printl(message: "打开数据库失败!")
            dbQueue = FMDatabaseQueue()
        }
        
        checkAndUpgradeDatabase(newDBVersion: self.newDBVersion)
    }
    
    deinit {
        dbQueue.close()
    }
    
    /// 建表，FMDatabase 参数为空时，内部会调用 dbQueue.inDatabase
    func createTable<T: DatabaseTable>(_ object: T.Type, db: FMDatabase? = nil) throws {
        //        if isExistTable(T.tableName) {
        //            printl(message: "表已存在")
        //            return
        //        }
        
        // 生成建表SQL
        let sql = try createTableSql(object)
        
        let reg = insertDataWithSQL(sql, values: [], db: db)
        if !reg {
            printl(message: "create \(T.tableName) failed")
            throw DatabaseError.tableCreationFailed
        }
    }
    
    func createTableSql<T: DatabaseTable>(_ object: T.Type) throws -> String {
        let mirrorType = T.initDummyInstance()
        let mirror = Mirror(reflecting: mirrorType)
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
                if T.enumPropertyMapper.keys.contains(label),
                   let enumType = T.enumPropertyMapper[label] {
                    switch enumType {
                    case .Int:
                        columnType = .integer
                    case .String:
                        columnType = .text
                    }
                } else {
                    columnType = .blob
                }
            default:
                throw DatabaseError.invalidType
            }
            
            columns.append(DBColumnInfo(name: label, type: columnType, isPrimaryKey: isPrimaryKey))
        }
        let createSQL = generateCreateTableSQL(tableName: T.tableName, columns: columns)
        return createSQL
    }
    
    // 插入数据
    func insertOrUpdate<T: DatabaseTable>(object: T, clear: Bool = false) throws {
        try insertOrUpdate(objects: [object])
    }
    
    /// insert objects
    func insertOrUpdate<T: DatabaseTable>(objects: [T], clear: Bool = false) throws {
//        if !isExistTable(T.tableName) {
//            printl(message: "不存在表，开始创建")
//            try createTable(T.self)
//        }
        
        if clear {
            deleteTable(from: T.tableName)
        }
        
        var insertTuples: [InsertTransactionTuple] = []
        
        for object in objects {
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
                case is Codable.Type, is Optional<Codable>.Type:
                    if let codableValue = child.value as? Codable {
                        if T.enumPropertyMapper.keys.contains(label),
                           let enumType = T.enumPropertyMapper[label] {
                            let rawRepresentableValue = child.value as? (any RawRepresentable)
                            switch enumType {
                            case .Int:
                                if let intValue = rawRepresentableValue?.rawValue as? Int {
                                    values.append(intValue)
                                }
                            case .String:
                                if let stringValue = rawRepresentableValue?.rawValue as? String {
                                    values.append(stringValue)
                                }
                            }
                        } else {
                            do {
                                let data = try JSONEncoder().encode(codableValue)
                                values.append(data)
                            } catch {
                                throw DatabaseError.encodingFailed
                            }
                        }
                        
                    }
                default:
                    throw DatabaseError.invalidType
                }
            }
            
            // 生成插入SQL
            let insertSQL = generateInsertOrUpdateSQL(tableName: T.tableName, columns: columns)
            
            insertTuples.append((insertSQL, values))
        }
        
        let reg = insertDataTransactionWithSQLTuples(insertTuples)
        if !reg {
            printl(message: "insert failed")
            throw DatabaseError.insertionFailed
        }
    }
    
    // 查询数据
    func query<T: DatabaseTable>(where condition: String? = nil) throws -> [T] {
        var sql = "SELECT * FROM \(T.tableName)"
        if let condition = condition {
            sql += " WHERE \(condition)"
        }
        
        var results: [T] = []
        
        let tempArray = getDataBySQL(sql, values: [])
        
        for dic in tempArray {
            var dictionary: [String: Any] = dic
            /// 注意这里
            let mirrorObjectType = T.initDummyInstance()
            let mirror = Mirror(reflecting: mirrorObjectType)
            
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
            let model = try JSONDecoder().decode(T.self, from: jsonData)
            results.append(model)
        }
        
        return results
    }
    
    @discardableResult
    func deleteTable(from tableName: String, otherSqlDic sqlDic: [String: String]? = nil) -> Bool {
        var deleteSql = "DELETE FROM \(tableName)"
        
        if let sqlDic = sqlDic, !sqlDic.isEmpty {
            deleteSql.append(" WHERE")
            
            for (key, value) in sqlDic {
                deleteSql.append(" \(key) = '\(value)'")
            }
        }
        
        printl(message: "Delete data SQL: \(deleteSql)")
        
        return deleteDataWithSQL(deleteSql, values: [])
    }
    
}

// MARK: - sql语句拼接，以及执行
extension DatabaseManager {
    /// 查询
    fileprivate func getDataBySQL(_ sql: String, values: [Any]) -> [[String: Any]] {
        var results: [[String: Any]] = []
        dbQueue.inDatabase { db in
            db.shouldCacheStatements = true
            guard let resultSet = db.executeQuery(sql, withArgumentsIn: values) else {
                printl(message: "未从数据库查询到数据")
                return
            }
            if db.hadError() {
                printl(message: "error \(db.lastErrorCode()) : \(db.lastErrorMessage())")
            }
            
            while resultSet.next() {
                if let dic = resultSet.resultDictionary as? [String: Any] {
                    results.append(dic)
                }
            }
        }
        return results
    }
    
    // 插入
    fileprivate func insertDataWithSQL(_ sql: String, values: [Any], db: FMDatabase? = nil) -> Bool {
        var result: Bool = true
        if let db = db {
            excuting(db: db)
        } else {
            dbQueue.inDatabase { db in
                excuting(db: db)
            }
        }
        
        func excuting(db: FMDatabase) {
            db.shouldCacheStatements = true
            result = db.executeUpdate(sql, withArgumentsIn: values)
            if db.hadError() {
                printl(message: "error \(db.lastErrorCode()) : \(db.lastErrorMessage())")
            }
        }
        
        return result
    }
    
    fileprivate func insertDataTransactionWithSQLTuples(_ tuples: [InsertTransactionTuple]) -> Bool {
        var result: Bool = true
        dbQueue.inTransaction { db, rollback in
            db.shouldCacheStatements = true
            for tuple in tuples {
                db.executeUpdate(tuple.sql, withArgumentsIn: tuple.values)
            }
            if db.hadError() {
                result = false
                rollback.pointee = true
                printl(message: "error \(db.lastErrorCode()) : \(db.lastErrorMessage())")
            }
        }
        return result
    }
    
    // 删除
    fileprivate func deleteDataWithSQL(_ sql: String, values: [Any]) -> Bool {
        var result: Bool = true
        dbQueue.inDatabase { db in
            db.shouldCacheStatements = true
            result = db.executeUpdate(sql, withArgumentsIn: values)
            if db.hadError() {
                printl(message: "error \(db.lastErrorCode()) : \(db.lastErrorMessage())")
            }
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
    fileprivate func generateInsertOrUpdateSQL(tableName: String, columns: [String]) -> String {
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
                            printl(message: "Remove \(tempPath) Success")
                        } catch {
                            printl(message: "Error removing \(tempPath): \(error.localizedDescription)")
                        }
                    }
                }
            } catch {
                printl(message: "Error retrieving contents of directory: \(error.localizedDescription)")
            }
        }
    }
    
    static func deleteFoldersContainingDB() {
        DispatchQueue.global(qos: .default).async {
            guard let cachesDirectory = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first else {
                printl(message: "无法获取 Caches 目录")
                return
            }
            
            do {
                let contents = try FileManager.default.contentsOfDirectory(
                    at: cachesDirectory,
                    includingPropertiesForKeys: [.isDirectoryKey],
                    options: .skipsHiddenFiles
                )
                
                for url in contents {
                    let resourceValues = try url.resourceValues(forKeys: [.isDirectoryKey])
                    if let isDirectory = resourceValues.isDirectory, isDirectory,
                       url.lastPathComponent.contains("DB") {
                        try FileManager.default.removeItem(at: url)
                        printl(message: "✅ 已删除文件夹: \(url.lastPathComponent)")
                    }
                }
            } catch {
                printl(message: "❌ 操作失败: \(error.localizedDescription)")
            }
        }
    }
}

// MARK: - 数据库升级
extension DatabaseManager {
    /// 子类重写：创建表
    func createTables(in db: FMDatabase) {
        try? createTable(User.self, db: db)
    }
    
    private func checkAndUpgradeDatabase(newDBVersion: String) {
        let oldVersion = UserDefaults.standard.string(forKey: dbVersionKey) ?? ""
        printl(message: "数据库版本: 旧 \(oldVersion) -> 新 \(newDBVersion)")
        
        if !oldVersion.isEmpty, oldVersion != newDBVersion {
            printl(message: "升级数据库")
            
            dbQueue.inDatabase { db in
                // 1. 获取旧表名
                let oldTables = self.getExistingTables(in: db)
                
                var backupTables: [String] = []
                
                // 2. 重命名旧表为 _bak
                for tableName in oldTables {
                    let backupName = tableName + "_bak"
                    let sql = "ALTER TABLE \(tableName) RENAME TO \(backupName)"
                    if db.executeUpdate(sql, withArgumentsIn: []) {
                        backupTables.append(backupName)
                        printl(message: "🔄 表 \(tableName) 已备份为 \(backupName)")
                    } else {
                        printl(message: "❌ 备份失败: \(db.lastErrorMessage())")
                    }
                }
                
                self.createTables(in: db)
                
                // 获取新表名
                let newTables = self.getNewTables(in: db)
                
                for newTableName in newTables {
                    let backupName = newTableName + "_bak"
                    if backupTables.contains(backupName) {
                        let oldCols = self.getTableColumns(tableName: backupName, in: db)
                        let newCols = self.getTableColumns(tableName: newTableName, in: db)
                        let commonCols = oldCols.intersection(newCols).sorted()
                        
                        if !commonCols.isEmpty {
                            let cols = commonCols.joined(separator: ", ")
                            let insertSQL = """
                                    INSERT INTO \(newTableName)(\(cols))
                                    SELECT \(cols) FROM \(backupName)
                                """
                            if db.executeUpdate(insertSQL, withArgumentsIn: []) {
                                printl(message: "✅ 数据从 \(backupName) 迁移到 \(newTableName)，字段: \(cols)")
                            } else {
                                printl(message: "❌ 数据迁移失败: \(db.lastErrorMessage())")
                            }
                        }
                    }
                }
                
                // 删除所有备份表
                db.beginTransaction()
                for backupTable in backupTables {
                    let dropSQL = "DROP TABLE IF EXISTS \(backupTable)"
                    db.executeUpdate(dropSQL, withArgumentsIn: [])
                }
                db.commit()
                
                UserDefaults.standard.set(newDBVersion, forKey: dbVersionKey)
                
                printl(message: "数据库升级完成")
            }
        } else {
            printl(message: "初始化数据库")
            dbQueue.inDatabase { db in
                self.createTables(in: db)
                
                UserDefaults.standard.set(newDBVersion, forKey: dbVersionKey)
            }
        }
    }
    
    private func getExistingTables(in db: FMDatabase) -> [String] {
        var tables: [String] = []
        let sql = "SELECT name FROM sqlite_master WHERE type='table'"
        
        if let rs = db.executeQuery(sql, withArgumentsIn: []) {
            while rs.next() {
                if let name = rs.string(forColumn: "name") {
                    tables.append(name)
                    printl(message: "🔍 发现表: \(name)")
                }
            }
            rs.close()
        } else {
            printl(message: "❌ 查询表失败: \(db.lastErrorMessage())")
        }
        
        printl(message: "📊 共发现 \(tables.count) 个表: \(tables)")
        return tables
    }
    
    private func getNewTables(in db: FMDatabase) -> [String] {
        var tables: [String] = []
        let sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '%_bak'"
        if let rs = db.executeQuery(sql, withArgumentsIn: []) {
            while rs.next() {
                if let name = rs.string(forColumn: "name") {
                    tables.append(name)
                }
            }
        }
        return tables
    }
    
    private func getTableColumns(tableName: String, in db: FMDatabase) -> Set<String> {
        var columns: Set<String> = []
        let sql = "PRAGMA table_info(\(tableName))"
        if let rs = db.executeQuery(sql, withArgumentsIn: []) {
            while rs.next() {
                if let colName = rs.string(forColumn: "name") {
                    columns.insert(colName)
                }
            }
        }
        return columns
    }
}


// MARK: - data扩展
extension Data {
    func jsonObject(options opt: JSONSerialization.ReadingOptions = []) throws -> Any? {
        return try? JSONSerialization.jsonObject(with: self, options: opt)
    }
}
