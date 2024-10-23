//
//  DebugPrint.swift
//  WoKeBang
//
//  Created by Apple on 2021/7/22.
//
import Foundation

func printl<T>(message: T, file: String = #file, funcName: String = #function, lineName: Int = #line) {
    #if DEBUG
    let fileName = (file as NSString).lastPathComponent
    let formter = DateFormatter()
    formter.dateFormat = "yyyy-MM-dd HH:mm:ss"
    let printDate = formter.string(from: Date())
    print("\(printDate): \(fileName) \(funcName) 第\(lineName)行: \(message)")
    #endif
}
