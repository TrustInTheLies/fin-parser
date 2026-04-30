use std::io::Write;

use crate::{
    Record, TxType, WriteError, bin::YPBankBinRecord, csv::YPBankCsvRecord, txt::YPBankTxtRecord,
};

pub(crate) fn write_csv<W: Write>(from: &Record, writer: &mut W) -> Result<(), WriteError> {
    let new_record = match from {
        Record::Bin(ypbank_bin_record) => YPBankCsvRecord::from(ypbank_bin_record),
        Record::Csv(ypbank_csv_record) => ypbank_csv_record.to_owned(),
        Record::Txt(ypbank_txt_record) => YPBankCsvRecord::from(ypbank_txt_record),
    };
    verify_user_ids(from)?;
    verify_description(from)?;
    let line = format!(
        "{},{},{},{},{},{},{},{}\n",
        new_record.body.0.0,
        new_record.body.1,
        new_record.body.2.0,
        new_record.body.3.0,
        new_record.body.4.0,
        new_record.body.5.0,
        new_record.body.6,
        new_record.write_description()?,
    );
    writer
        .write_all(line.as_bytes())
        .map_err(|e| WriteError::FailedWriter(format!("Failed to write: {}", e)))?;
    Ok(())
}

pub(crate) fn write_bin<W: Write>(from: &Record, writer: &mut W) -> Result<(), WriteError> {
    let new_record = match from {
        Record::Bin(ypbank_bin_record) => ypbank_bin_record.to_owned(),
        Record::Csv(ypbank_csv_record) => YPBankBinRecord::from(ypbank_csv_record),
        Record::Txt(ypbank_txt_record) => YPBankBinRecord::from(ypbank_txt_record),
    };
    verify_user_ids(from)?;
    verify_description(from)?;
    writer
        .write_all(&new_record.build_data()?)
        .map_err(|e| WriteError::FailedWriter(format!("Failed to write: {}", e)))?;
    Ok(())
}

pub(crate) fn write_txt<W: Write>(from: &Record, writer: &mut W) -> Result<(), WriteError> {
    let new_record = match from {
        Record::Bin(ypbank_bin_record) => YPBankTxtRecord::from(ypbank_bin_record),
        Record::Csv(ypbank_csv_record) => YPBankTxtRecord::from(ypbank_csv_record),
        Record::Txt(ypbank_txt_record) => ypbank_txt_record.to_owned(),
    };
    verify_user_ids(from)?;
    verify_description(from)?;
    let line = format!(
        "TX_ID: {}\nTX_TYPE: {}\nFROM_USER_ID: {}\nTO_USER_ID: {}\nAMOUNT: {}\nTIMESTAMP: {}\nSTATUS: {}\nDESCRIPTION: {}\n\n",
        new_record.body.0.0,
        new_record.body.1,
        new_record.body.2.0,
        new_record.body.3.0,
        new_record.body.4.0,
        new_record.body.5.0,
        new_record.body.6,
        new_record.write_description()?,
    );
    writer
        .write_all(line.as_bytes())
        .map_err(|e| WriteError::FailedWriter(format!("Failed to write: {}", e)))?;
    Ok(())
}

fn verify_user_ids(record: &Record) -> Result<(), WriteError> {
    let body = match record {
        Record::Bin(ypbank_bin_record) => ypbank_bin_record.body.to_owned(),
        Record::Csv(ypbank_csv_record) => ypbank_csv_record.body.to_owned(),
        Record::Txt(ypbank_txt_record) => ypbank_txt_record.body.to_owned(),
    };
    if matches!(body.1, TxType::Deposit) && body.2.0 != 0 {
        return Err(WriteError::IncorrectData(format!(
            "FROM_USER_ID is {} when it should be 0 cause of TX_TYPE = DEPOSIT",
            body.2.0
        )));
    }
    if matches!(body.1, TxType::Withdrawal) && body.3.0 != 0 {
        return Err(WriteError::IncorrectData(format!(
            "TO_USER_ID is {} when it should be 0 cause of TX_TYPE = WITHDRAWAL",
            body.3.0
        )));
    }
    if body.2.0 == 0 && body.3.0 == 0 {
        return Err(WriteError::IncorrectData(
            "At lease one user must be present".to_string(),
        ));
    }
    Ok(())
}

fn verify_description(record: &Record) -> Result<(), WriteError> {
    let body = match record {
        Record::Bin(ypbank_bin_record) => ypbank_bin_record.body.to_owned(),
        Record::Csv(ypbank_csv_record) => ypbank_csv_record.body.to_owned(),
        Record::Txt(ypbank_txt_record) => ypbank_txt_record.body.to_owned(),
    };
    match record {
        Record::Bin(_) => {
            if let Some(desc) = body.8.0 {
                if body.7.0.is_none() {
                    return Err(WriteError::IncorrectData(
                        "DESC_LEN value is missing".to_string(),
                    ));
                };
                if body
                    .7
                    .0
                    .is_some_and(|desc_len| desc_len != desc.len() as u32)
                {
                    return Err(WriteError::IncorrectData(
                        "DESC_LEN value doesn't match DESCRIPTION length".to_string(),
                    ));
                };
            } else {
                if body.7.0.is_some() {
                    return Err(WriteError::IncorrectData(
                        "DESC_LEN value presented while DESCRIPTION is missing".to_string(),
                    ));
                }
            }
            Ok(())
        }
        _ => {
            if body.7.0.is_some() {
                return Err(WriteError::IncorrectData(
                    "DESC_LEN value presented while it shouldn't be".to_string(),
                ));
            }
            Ok(())
        }
    }
}
