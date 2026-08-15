package main

import (
	"bytes"
	"embed"
	"flag"
	"fmt"
	"log"
	"os"
	"text/template"

	"github.com/VKCOM/statshouse/internal/duckstore"
)

type SchemaParams struct {
	BasicTags  []int
	InputTable bool
}

type TableTTL struct {
	DaysToDisk int
	DiskName   string
	Days       int
	Hours      int
}

func (t TableTTL) String() string {
	if t.DaysToDisk <= 0 && t.Days <= 0 && t.Hours <= 0 {
		return ""
	}
	if t.Days > 0 && t.Hours > 0 {
		log.Fatalln("can't simultaneously set TTL in days and hours")
	}
	r := "TTL "
	needComma := false
	if t.DaysToDisk > 0 {
		r += fmt.Sprintf("time + toIntervalDay(%d) TO DISK '%s'", t.DaysToDisk, t.DiskName)
		needComma = true
	}
	if t.Days > 0 {
		if needComma {
			r += ", "
		}
		r += fmt.Sprintf("time + toIntervalDay(%d)", t.Days)
	}
	if t.Hours > 0 {
		r += fmt.Sprintf("time + toIntervalHour(%d)", t.Hours)
	}
	return r
}

type TableSettings struct {
	IntSettings map[string]int
	StrSettings map[string]string
}

func (t TableSettings) String() string {
	if len(t.IntSettings) == 0 && len(t.StrSettings) == 0 {
		return ""
	}
	r := "SETTINGS "
	needComma := false
	for k, v := range t.IntSettings {
		if needComma {
			r += ", "
		}
		r += fmt.Sprintf("%s = %d", k, v)
		needComma = true
	}
	for k, v := range t.StrSettings {
		if needComma {
			r += ", "
		}
		r += fmt.Sprintf("%s = '%s'", k, v)
		needComma = true
	}
	return r
}

type TablePartition struct {
	month bool
	day   bool
	hours int
}

func (t TablePartition) String() string {
	if t.month {
		return "PARTITION BY toYYYYMM(time)"
	}
	if t.day {
		return "PARTITION BY toDate(time)"
	}
	if t.hours > 0 {
		return fmt.Sprintf("PARTITION BY toStartOfInterval(time, toIntervalHour(%d))", t.hours)
	}
	return ""
}

type TableParams struct {
	NamePrefix  string
	NamePostfix string
	Resolution  string
	Cluster     string
	Schema      SchemaParams
	SelectFrom  string
	TTL         TableTTL
	Partition   TablePartition
	Settings    TableSettings
}

type IncomingTableParams struct {
	NamePrefix  string
	NamePostfix string
	Cluster     string
	Schema      SchemaParams
}

type Params struct {
	IncomingTable IncomingTableParams
	Tables        []TableParams
}

func (itp IncomingTableParams) tableName() string {
	return itp.NamePrefix + itp.NamePostfix
}

func parseParams(args []string) (params Params, err error) {
	var schemaParams SchemaParams
	var basicTagsN int
	var stringTags bool
	var cluster string
	var partitionHours int
	var tablesPrefix string
	var backend duckstore.StorageBackend
	const incomingPostfix = "incoming"
	f := flag.NewFlagSet("ch-table-gen", flag.ContinueOnError)
	f.IntVar(&basicTagsN, "basic-tags", 48, "number of basic tags")
	f.BoolVar(&stringTags, "string-tags", true, "basic tags can be stored as unmapped strings")
	f.IntVar(&partitionHours, "partition-hours", 24, "partition by that many hours")
	f.StringVar(&cluster, "cluster", "statlogs2", "clickhouse cluster name")
	f.StringVar(&tablesPrefix, "prefix", "statshouse_v6_", "prefix for tables")
	f.Var(&backend, "storage-backend", "storage backend the generated tables are for: \"clickhouse\" (default). \"duck\" is rejected: duck-store creates its own schema on first start and has no ClickHouse tables to generate")
	if err := f.Parse(args); err != nil {
		return params, err
	}
	// This is ClickHouse-only tooling: it emits the DDL for the ClickHouse
	// cluster's v6 tables. Invoked against the duck backend it must hard-error
	// rather than print DDL no duck deployment would execute — under duck the
	// store owns its schema and creates it on first start.
	if backend == duckstore.BackendDuck {
		return params, fmt.Errorf("--storage-backend=duck: the table generator is ClickHouse-only tooling, and duck-store needs no generated tables (its store creates the schema on first start)")
	}

	schemaParams.BasicTags = make([]int, basicTagsN)
	for i := 0; i < basicTagsN; i++ {
		schemaParams.BasicTags[i] = i
	}

	incomingSchemaParams := schemaParams
	incomingSchemaParams.InputTable = true
	commonSettings := TableSettings{
		IntSettings: make(map[string]int),
		StrSettings: make(map[string]string),
	}
	commonSettings.IntSettings["index_granularity"] = 8192
	commonSettings.IntSettings["ttl_only_drop_parts"] = 1
	commonSettings.StrSettings["storage_policy"] = "ssd_then_hdd"
	secSettings := TableSettings{
		IntSettings: make(map[string]int),
		StrSettings: make(map[string]string),
	}
	for k, v := range commonSettings.IntSettings {
		secSettings.IntSettings[k] = v
	}
	for k, v := range commonSettings.StrSettings {
		secSettings.StrSettings[k] = v
	}
	secSettings.IntSettings["max_bytes_to_merge_at_max_space_in_pool"] = 16106127360

	incomingTable := IncomingTableParams{
		NamePrefix:  "statshouse_v3_",
		NamePostfix: incomingPostfix,
		Cluster:     cluster,
		Schema:      incomingSchemaParams,
	}
	params = Params{
		IncomingTable: incomingTable,
		Tables: []TableParams{
			{
				NamePrefix: tablesPrefix,
				Resolution: "1s",
				Cluster:    cluster,
				Schema:     schemaParams,
				SelectFrom: incomingTable.tableName(),
				TTL: TableTTL{
					Hours: 52,
				},
				Partition: TablePartition{
					hours: 24,
				},
				Settings: secSettings,
			},
			{
				NamePrefix: tablesPrefix,
				Resolution: "1m",
				Cluster:    cluster,
				Schema:     schemaParams,
				SelectFrom: incomingTable.tableName(),
				TTL: TableTTL{
					DaysToDisk: 4,
					DiskName:   "default",
					Days:       33,
				},
				Partition: TablePartition{
					day: true,
				},
				Settings: commonSettings,
			},
			{
				NamePrefix: tablesPrefix,
				Resolution: "1h",
				Cluster:    cluster,
				Schema:     schemaParams,
				SelectFrom: incomingTable.tableName(),
				TTL: TableTTL{
					DaysToDisk: 4,
					DiskName:   "default",
				},
				Partition: TablePartition{
					month: true,
				},
				Settings: commonSettings,
			},
		},
	}

	return params, nil
}

//go:embed init-statshouse.go.tmpl resolution-tables.go.tmpl table-schema.go.tmpl table-order.go.tmpl
var embedTemplates embed.FS

func main() {
	params, err := parseParams(os.Args[1:])
	if err != nil {
		log.Fatal("failed to parse parameters:", err)
	}

	tmpl, err := template.ParseFS(embedTemplates, "*.go.tmpl")
	if err != nil {
		log.Fatal("failed to parse template file table.go.tmpl:", err)
	}

	buffer := new(bytes.Buffer)
	err = tmpl.ExecuteTemplate(buffer, "init-statshouse.go.tmpl", params)
	if err != nil {
		log.Fatal("failed to render template:", err)
	}
	fmt.Print(buffer)
}
