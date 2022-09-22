package util

import "log"

func CheckErr(remark string, err error) {
    if err != nil {
        log.Fatal(remark + ":" + err.Error())
    }
}