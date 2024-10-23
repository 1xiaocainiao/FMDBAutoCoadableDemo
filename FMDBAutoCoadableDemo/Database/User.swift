//
//  File.swift
//  WebServiceMoyaCodableResultDemo
//
//  Created by mac on 2024/10/22.
//

import Foundation
// 定义模型
class User: DatabaseTable {
    static var tableName: String { return "users" }
    static func primaryKey() -> String { return "id" }
    
    var id: Int?
    var name: String?
    var profile: [Profile]? // 嵌套模型
    var isSelf: Bool?
}

class Profile: Codable {
    var age: Int?
    var email: String?
}


//struct Address: Codable {
//    var city: String
//    var street: String
//}
//
//struct User: Codable {
//    var name: String
//    var age: Int
//    var address: Address
//}


