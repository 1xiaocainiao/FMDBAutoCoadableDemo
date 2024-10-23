//
//  ViewController.swift
//  FMDBAutoCoadableDemo
//
//  Created by mac on 2024/10/23.
//

import UIKit

class ViewController: UIViewController {

    override func viewDidLoad() {
        super.viewDidLoad()
        
        testDataBase()
        // Do any additional setup after loading the view.
    }

    func testDataBase() {
        do {
            let dbManager = DatabaseManager()
            
            // 创建表
            try dbManager.createTable(User())
            
            // 插入数据
            let profile = Profile()
            profile.age = 25
            profile.email = "test@example.com"
            let user = User()
            user.id = 1
            user.name = "张三 李四 王麻子"
            user.profile = [profile]
            user.isSelf = true
//            try dbManager.insert(user)
            
            _ = dbManager.deleteTable(from: User.tableName, otherSqlDic: [:])
            
            let temp = try dbManager.query(User(), where: "id = 1")
            print(temp)
            
        } catch {
            print("Error: \(error)")
        }
    }

}

