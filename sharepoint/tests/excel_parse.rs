//! Excel (xlsx) parsing — builds a workbook in memory with `rust_xlsxwriter`,
//! then parses it back through the plugin's calamine path. Covers headers,
//! `skip_rows`, sheet selection, and type inference into Arrow.

use loadsmith_sharepoint::config::ParserConfig;
use loadsmith_sharepoint::parse::parse;
use rust_xlsxwriter::Workbook;

fn workbook_bytes() -> Vec<u8> {
    let mut wb = Workbook::new();
    let ws = wb.add_worksheet().set_name("DADOS").unwrap();
    // Two banner rows to skip, then a header row, then data.
    ws.write(0, 0, "relatório").unwrap();
    ws.write(1, 0, "gerado em 2026").unwrap();
    ws.write(2, 0, "ATUAÇÃO").unwrap();
    ws.write(2, 1, "QTD").unwrap();
    ws.write(3, 0, "civel").unwrap();
    ws.write(3, 1, 10).unwrap();
    ws.write(4, 0, "trab").unwrap();
    ws.write(4, 1, 20).unwrap();
    wb.save_to_buffer().unwrap()
}

#[test]
fn excel_with_skiprows_header_and_sheet() {
    let bytes = workbook_bytes();
    let parser = ParserConfig::Excel {
        sheet: Some("DADOS".to_string()),
        header: true,
        skip_rows: 2,
    };
    let table = parse(&bytes, &parser).unwrap();
    assert_eq!(table.columns, vec!["ATUAÇÃO", "QTD"]);
    assert_eq!(table.num_rows(), 2);

    // Excel stores all numbers as IEEE doubles, so calamine surfaces them as
    // floats — QTD infers to Float64 (the truthful Excel representation).
    let (_schema, batches) = table.to_record_batches(1000).unwrap();
    let schema = batches[0].schema();
    assert_eq!(
        schema.field(1).data_type(),
        &arrow_schema::DataType::Float64
    );
}
