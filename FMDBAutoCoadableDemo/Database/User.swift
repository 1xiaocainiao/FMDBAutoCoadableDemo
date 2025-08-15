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
//    var avatar: String?
    
    var fileType: FilterType? // 自定义枚举
    
    static var enumPropertyMapper: [String: DBModelEnumType] {
        return ["fileType": .String]
    }
}

class Profile: Codable {
    var age: Int?
    var email: String?
    
    var address: Address?
}

enum FilterType: String, Codable {
    case name
    case age
}


class Address: Codable {
    var city: String?
    var street: String?
}

