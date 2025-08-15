//
//  ViewController.swift
//  FMDBAutoCoadableDemo
//
//  Created by mac on 2024/10/23.
//

import UIKit

class ViewController: UIViewController {
    var dbManager: DatabaseManager!

    override func viewDidLoad() {
        super.viewDidLoad()
        
//        DatabaseManager.deleteFoldersContainingDB()
        
//        testOneDataBase()
        
        dbManager = DatabaseManager()
        DispatchQueue.main.asyncAfter(deadline: .now() + 0) {
            self.testDataBaseArray()
            
//            self.testUpdateDB()
        }
        // Do any additional setup after loading the view.
    }

    func testOneDataBase() {
        do {
            
//            // 创建表
//            try dbManager.createTable(User.self)
            
            let adress = Address()
            adress.city = "北京"
            adress.street = "朝阳区"
            // 插入数据
            let profile = Profile()
            profile.age = 25
            profile.email = "test@example.com"
            profile.address = adress
            let user = User()
            user.id = 1
            user.name = "张三 李四 王麻子"
            user.profile = [profile]
            user.isSelf = true
            user.fileType = .name
            try dbManager.insertOrUpdate(object: user)
            
//            _ = dbManager.deleteTable(from: User.tableName, otherSqlDic: [:])
            
//            let temp = try dbManager.query(User(), where: "id = 1")
//            print(temp)
            
        } catch {
            print("Error: \(error)")
        }
    }
    
    func testDataBaseArray() {
        do {
            
//            // 创建表
//            try dbManager.createTable(User.self)
            
            var results: [User] = []
            for index in 0..<20 {
                let adress = Address()
                adress.city = "北京"
                adress.street = "朝阳区"
                // 插入数据
                let profile = Profile()
                profile.age = 25
                profile.email = "test\(index)@example.com"
                profile.address = adress
                let user = User()
                user.id = index
                user.name = "张三 李四 王麻子__\(index)"
                user.profile = [profile]
                user.isSelf = true
                user.fileType = .name
                
                results.append(user)
            }
            
            
            try dbManager.insertOrUpdate(objects: results)
            
//            _ = dbManager.deleteTable(from: User.tableName, otherSqlDic: [:])
            
            let temp: [User] = try dbManager.query()
            print(temp)
            
        } catch {
            print("Error: \(error)")
        }
    }
    
    func testUpdateDB() {
        let dbManager = DatabaseManager()
        
        do {
            let temp: [User] = try dbManager.query()
            print(temp)
        } catch {
            print("Error: \(error)")
        }
    }

}

