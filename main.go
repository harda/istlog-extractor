package main

import (
	"bufio"
	"database/sql"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	"fmt"
)

type shclog struct {
	id        int
	pan       sql.NullString
	acquirer  sql.NullString
	amount    int
	issuer    sql.NullString
	localdate sql.NullString
	localtime int
	trace     int
	refnum    sql.NullString
	termid    sql.NullString
}

type Shclogs []shclog

func main() {

	fmt.Fprintf(os.Stderr, "command: %s %s\n", os.Args[0], os.Args[1])

	db, err := sql.Open("mysql", "root:r00t@tcp([127.0.0.1]:3306)/test")
	if err != nil {
		log.Fatal("Could not connect, error ", err.Error())
	}
	defer db.Close()

	cw := GetShclogById(db, 1)
	fmt.Println(cw)

	ca := GetShclogAll(db, []string{"'500100'", "'600100'"})
	fmt.Println("got the result")
	fmt.Println(ca)

	file1, err := os.OpenFile("istlogbydate.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Error happened while processing file", err)
	}

	writebuffer := bufio.NewWriter(file1)
	for i := 0; i < len(ca); i++ {
		writebuffer.WriteString(fmt.Sprintln("File", GetValueWithSpace(ca[i].acquirer, 20), GetValueWithSpace(ca[i].termid, 20),
			GetValueWithSpace(ca[i].issuer, 20), ca[i].amount))
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

func GetShclogAll(db *sql.DB, acquirer []string) Shclogs {

	Qs := fmt.Sprintf("SELECT id,Acquirer, Issuer, Amount,Termid from SHCLOG where Acquirer in (%s);", strings.Join(acquirer, ","))

	rows, err := db.Query(Qs)
	if err != nil {
		log.Fatal("Could not get data from the Shclog table ", err)
	}
	defer rows.Close()

	retVal := Shclogs{}
	cols, _ := rows.Columns()
	fmt.Println("Columns detected: ", cols)

	for rows.Next() {
		member := shclog{}
		err = rows.Scan(&member.id, &member.acquirer, &member.issuer, &member.amount, &member.termid)
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

func GetShclogById(db *sql.DB, id int) (cm shclog) {
	row := db.QueryRow("Select id, acquirer, amount, issuer, local_time, trace, refnum, termid from SHCLOG where id = ?", id)

	err := row.Scan(&cm.id, &cm.acquirer, &cm.amount, &cm.issuer, &cm.localtime, &cm.trace, &cm.refnum, &cm.termid)
	if err != nil {
		fmt.Println("error!!")
		log.Fatal(err)
	}
	return
}
