package main

import (
	"bufio"
	"database/sql"
	"log"
	"os"

	// _ "github.com/go-sql-driver/mysql"
	_ "github.com/sijms/go-ora"

	"fmt"
)

type shclog struct {
	pan      sql.NullString
	acquirer sql.NullString
	// amount    int
	issuer sql.NullString
	// localdate sql.NullString
	// localtime int
	// trace    int
	refnum sql.NullString
	termid sql.NullString
	// respcode int
}

type Shclogs []shclog

func main() {

	if len(os.Args) < 2 {
		fmt.Println("Parameter error, please type the correct syntax!")
	}
	dateStr := os.Args[1]

	// db, err := sql.Open("mysql", "root:r00t@tcp([127.0.0.1]:3306)/test")
	// if err != nil {
	// 	log.Fatal("Could not connect, error ", err.Error())
	// }
	// defer db.Close()

	db, err := sql.Open("oracle", "oracle://ist77:ist77@10.10.77.39:1521/orcl")
	if err != nil {
		log.Fatal("Could not connect, error ", err.Error())
	}
	defer db.Close()

	ca := GetShclogAll(db, dateStr)
	fmt.Println("got the result")
	fmt.Println(ca)

	file1, err := os.OpenFile("istlogbydate.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Error happened while processing file", err)
	}

	writebuffer := bufio.NewWriter(file1)
	for i := 0; i < len(ca); i++ {
		writebuffer.WriteString(fmt.Sprintln("File", GetValueWithSpace(ca[i].acquirer, 20), GetValueWithSpace(ca[i].termid, 20),
			GetValueWithSpace(ca[i].issuer, 20)))
	}
	writebuffer.Flush()

}

func GetValueWithSpace(value sql.NullString, lenght int) string {

	var result string
	if value.Valid {
		result = value.String
	}

	for {
		result = result + " "
		if len(result) > lenght {
			break
		}
	}

	return result
}

func GetShclogAll(db *sql.DB, dateStr string) Shclogs {

	stmt, err := db.Prepare("SELECT pan, Acquirer, Issuer, refnum,Termid from SHCLOG_REQ where MSGTYPE=210")
	if err != nil {
		log.Fatal("Could prepare statement ", err)
	}
	defer stmt.Close()

	//mysql
	// Qs := fmt.Sprintf("SELECT pan, Acquirer, Issuer, refnum,Termid from SHCLOG_REQ where LOCAL_DATE = TO_DATE(%s,'DD-MM-YYYY') AND MSGTYPE=210;", dateStr)
	// rows, err := db.Query(Qs)
	// if err != nil {
	// 	log.Fatal("Could not get data from the Shclog table ", err)
	// }
	// defer rows.Close()

	rows, err := stmt.Query()
	if err != nil {
		log.Fatal("Could not get data from the Shclog table ", err)
	}
	defer rows.Close()

	retVal := Shclogs{}
	cols, _ := rows.Columns()
	fmt.Println("Columns detected: ", cols)

	for rows.Next() {
		member := shclog{}
		err = rows.Scan(&member.pan, &member.acquirer, &member.issuer, &member.termid)
		if err != nil {
			log.Fatal("Error scanning row", err)
		}
		retVal = append(retVal, member)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	return retVal
}
