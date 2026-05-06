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

mod tests {
    #[cfg(test)]
    use crate::{
        Amount, Body, DescLen, Description, FromUserId, Head, Record, Status, Timestamp, ToUserId,
        TxId, TxType,
        bin::{MAGIC, SIZE_WITHOUT_DESCRIPTION, YPBankBinRecord},
        csv::YPBankCsvRecord,
        txt::YPBankTxtRecord,
        write::verify_description,
        write::verify_user_ids,
        write::{write_bin, write_csv, write_txt},
    };
    #[cfg(test)]
    use std::io::{Error, Write};

    #[cfg(test)]
    #[allow(unused)]
    struct BrokenWriter(Vec<u8>);

    #[cfg(test)]
    impl Write for BrokenWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if buf.len() > 2 {
                Err(Error::new(std::io::ErrorKind::WriteZero, "test"))
            } else {
                Ok(buf.len())
            }
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[cfg(test)]
    fn create_head() -> Head {
        let record_size = SIZE_WITHOUT_DESCRIPTION + 3;
        Head {
            magic: u32::from_be_bytes(MAGIC),
            record_size,
        }
    }
    #[cfg(test)]
    fn create_body_with_desc(txtype: TxType, status: Status, size: Option<u32>) -> Body {
        Body(
            TxId(1),
            txtype,
            FromUserId(0),
            ToUserId(1),
            Amount(1),
            Timestamp(1),
            status,
            DescLen(size),
            Description(Some("oleg".as_bytes().to_vec())),
        )
    }

    #[cfg(test)]
    fn create_body_without_desc(txtype: TxType, status: Status) -> Body {
        Body(
            TxId(1),
            txtype,
            FromUserId(0),
            ToUserId(1),
            Amount(1),
            Timestamp(1),
            status,
            DescLen(None),
            Description(None),
        )
    }

    #[cfg(test)]
    fn create_txt_record(
        txtype: TxType,
        status: Status,
        size: Option<u32>,
        with_desc: bool,
    ) -> YPBankTxtRecord {
        YPBankTxtRecord {
            body: if with_desc {
                create_body_with_desc(txtype, status, size)
            } else {
                create_body_without_desc(txtype, status)
            },
        }
    }

    #[cfg(test)]
    fn create_csv_record(
        txtype: TxType,
        status: Status,
        size: Option<u32>,
        with_desc: bool,
    ) -> YPBankCsvRecord {
        YPBankCsvRecord {
            body: if with_desc {
                create_body_with_desc(txtype, status, size)
            } else {
                create_body_without_desc(txtype, status)
            },
        }
    }

    #[cfg(test)]
    fn create_bin_record(
        txtype: TxType,
        status: Status,
        size: Option<u32>,
        with_desc: bool,
    ) -> YPBankBinRecord {
        YPBankBinRecord {
            head: create_head(),
            body: if with_desc {
                create_body_with_desc(txtype, status, size)
            } else {
                create_body_without_desc(txtype, status)
            },
        }
    }

    #[test]
    fn test_verify_bin_description() {
        let bin_record = Record::Bin(create_bin_record(
            TxType::Deposit,
            Status::Failure,
            Some(4),
            true,
        ));
        let bin_record_wrong_size = Record::Bin(create_bin_record(
            TxType::Deposit,
            Status::Failure,
            Some(11),
            true,
        ));
        let bin_record_no_desc_len = Record::Bin(create_bin_record(
            TxType::Deposit,
            Status::Failure,
            None,
            true,
        ));
        let bin_record_no_desc = Record::Bin(YPBankBinRecord {
            head: create_head(),
            body: Body(
                TxId(1),
                TxType::Deposit,
                FromUserId(0),
                ToUserId(1),
                Amount(1),
                Timestamp(1),
                Status::Failure,
                DescLen(Some(11)),
                Description(None),
            ),
        });
        assert!(verify_description(&bin_record).is_ok());
        assert!(
            verify_description(&bin_record_wrong_size)
                .is_err_and(|e| e.to_string() == "DESC_LEN value doesn't match DESCRIPTION length")
        );
        assert!(verify_description(&bin_record_no_desc).is_err_and(
            |e| e.to_string() == "DESC_LEN value presented while DESCRIPTION is missing"
        ));
        assert!(
            verify_description(&bin_record_no_desc_len)
                .is_err_and(|e| e.to_string() == "DESC_LEN value is missing")
        );
    }

    #[test]
    fn test_verify_non_bin_description() {
        let csv_record = Record::Csv(create_csv_record(
            TxType::Deposit,
            Status::Failure,
            None,
            true,
        ));
        let txt_record = Record::Txt(create_txt_record(
            TxType::Deposit,
            Status::Failure,
            None,
            true,
        ));
        let csv_record_with_desc_len = Record::Csv(create_csv_record(
            TxType::Deposit,
            Status::Failure,
            Some(11),
            true,
        ));
        let txt_record_with_desc_len = Record::Txt(create_txt_record(
            TxType::Deposit,
            Status::Failure,
            Some(11),
            true,
        ));
        assert!(verify_description(&csv_record).is_ok());
        assert!(verify_description(&txt_record).is_ok());
        assert!(
            verify_description(&csv_record_with_desc_len)
                .is_err_and(|e| e.to_string() == "DESC_LEN value presented while it shouldn't be")
        );
        assert!(
            verify_description(&txt_record_with_desc_len)
                .is_err_and(|e| e.to_string() == "DESC_LEN value presented while it shouldn't be")
        );
    }

    #[test]
    fn test_verify_user_ids_success() {
        let bin_record = Record::Bin(create_bin_record(
            TxType::Deposit,
            Status::Failure,
            Some(4),
            true,
        ));
        let csv_record = Record::Csv(create_csv_record(
            TxType::Deposit,
            Status::Failure,
            None,
            true,
        ));
        let txt_record = Record::Txt(create_txt_record(
            TxType::Deposit,
            Status::Failure,
            None,
            true,
        ));
        assert!(verify_user_ids(&bin_record).is_ok());
        assert!(verify_user_ids(&csv_record).is_ok());
        assert!(verify_user_ids(&txt_record).is_ok());
    }

    #[test]
    fn test_verify_user_ids_from() {
        let bin_record = Record::Bin(YPBankBinRecord {
            head: create_head(),
            body: Body(
                TxId(1),
                TxType::Deposit,
                FromUserId(4),
                ToUserId(1),
                Amount(1),
                Timestamp(1),
                Status::Failure,
                DescLen(Some(11)),
                Description(None),
            ),
        });
        let csv_record = Record::Csv(YPBankCsvRecord {
            body: Body(
                TxId(1),
                TxType::Deposit,
                FromUserId(4),
                ToUserId(1),
                Amount(1),
                Timestamp(1),
                Status::Failure,
                DescLen(Some(11)),
                Description(None),
            ),
        });
        let txt_record = Record::Txt(YPBankTxtRecord {
            body: Body(
                TxId(1),
                TxType::Deposit,
                FromUserId(4),
                ToUserId(1),
                Amount(1),
                Timestamp(1),
                Status::Failure,
                DescLen(Some(11)),
                Description(None),
            ),
        });
        assert!(
            verify_user_ids(&bin_record).is_err_and(|e| e.to_string()
                == "FROM_USER_ID is 4 when it should be 0 cause of TX_TYPE = DEPOSIT")
        );
        assert!(
            verify_user_ids(&csv_record).is_err_and(|e| e.to_string()
                == "FROM_USER_ID is 4 when it should be 0 cause of TX_TYPE = DEPOSIT")
        );
        assert!(
            verify_user_ids(&txt_record).is_err_and(|e| e.to_string()
                == "FROM_USER_ID is 4 when it should be 0 cause of TX_TYPE = DEPOSIT")
        );
    }

    #[test]
    fn test_verify_user_ids_to() {
        let bin_record = Record::Bin(YPBankBinRecord {
            head: create_head(),
            body: Body(
                TxId(1),
                TxType::Withdrawal,
                FromUserId(4),
                ToUserId(1),
                Amount(1),
                Timestamp(1),
                Status::Failure,
                DescLen(Some(11)),
                Description(None),
            ),
        });
        let csv_record = Record::Csv(YPBankCsvRecord {
            body: Body(
                TxId(1),
                TxType::Withdrawal,
                FromUserId(4),
                ToUserId(1),
                Amount(1),
                Timestamp(1),
                Status::Failure,
                DescLen(Some(11)),
                Description(None),
            ),
        });
        let txt_record = Record::Txt(YPBankTxtRecord {
            body: Body(
                TxId(1),
                TxType::Withdrawal,
                FromUserId(4),
                ToUserId(1),
                Amount(1),
                Timestamp(1),
                Status::Failure,
                DescLen(Some(11)),
                Description(None),
            ),
        });
        assert!(verify_user_ids(&bin_record).is_err_and(|e| e.to_string()
            == "TO_USER_ID is 1 when it should be 0 cause of TX_TYPE = WITHDRAWAL"));
        assert!(verify_user_ids(&csv_record).is_err_and(|e| e.to_string()
            == "TO_USER_ID is 1 when it should be 0 cause of TX_TYPE = WITHDRAWAL"));
        assert!(verify_user_ids(&txt_record).is_err_and(|e| e.to_string()
            == "TO_USER_ID is 1 when it should be 0 cause of TX_TYPE = WITHDRAWAL"));
    }

    #[test]
    fn test_verify_user_ids_empty() {
        let bin_record = Record::Bin(YPBankBinRecord {
            head: create_head(),
            body: Body(
                TxId(1),
                TxType::Withdrawal,
                FromUserId(0),
                ToUserId(0),
                Amount(1),
                Timestamp(1),
                Status::Failure,
                DescLen(Some(11)),
                Description(None),
            ),
        });
        let csv_record = Record::Csv(YPBankCsvRecord {
            body: Body(
                TxId(1),
                TxType::Withdrawal,
                FromUserId(0),
                ToUserId(0),
                Amount(1),
                Timestamp(1),
                Status::Failure,
                DescLen(Some(11)),
                Description(None),
            ),
        });
        let txt_record = Record::Txt(YPBankTxtRecord {
            body: Body(
                TxId(1),
                TxType::Withdrawal,
                FromUserId(0),
                ToUserId(0),
                Amount(1),
                Timestamp(1),
                Status::Failure,
                DescLen(Some(11)),
                Description(None),
            ),
        });
        assert!(
            verify_user_ids(&bin_record)
                .is_err_and(|e| e.to_string() == "At lease one user must be present")
        );
        assert!(
            verify_user_ids(&csv_record)
                .is_err_and(|e| e.to_string() == "At lease one user must be present")
        );
        assert!(
            verify_user_ids(&txt_record)
                .is_err_and(|e| e.to_string() == "At lease one user must be present")
        );
    }

    #[test]
    fn test_write_txt() {
        let bin_record = Record::Bin(create_bin_record(
            TxType::Deposit,
            Status::Failure,
            Some(4),
            true,
        ));
        let csv_record = Record::Csv(create_csv_record(
            TxType::Deposit,
            Status::Failure,
            None,
            true,
        ));
        let txt_record = Record::Txt(create_txt_record(
            TxType::Deposit,
            Status::Failure,
            None,
            true,
        ));
        let mut writer = BrokenWriter(Vec::new());
        assert!(
            write_txt(&bin_record, &mut writer)
                .is_err_and(|e| e.to_string() == "Failed to write: test")
        );
        assert!(
            write_txt(&csv_record, &mut writer)
                .is_err_and(|e| e.to_string() == "Failed to write: test")
        );
        assert!(
            write_txt(&txt_record, &mut writer)
                .is_err_and(|e| e.to_string() == "Failed to write: test")
        );
    }

    #[test]
    fn test_write_bin() {
        let bin_record = Record::Bin(create_bin_record(
            TxType::Deposit,
            Status::Failure,
            Some(4),
            true,
        ));
        let csv_record = Record::Csv(create_csv_record(
            TxType::Deposit,
            Status::Failure,
            None,
            true,
        ));
        let txt_record = Record::Txt(create_txt_record(
            TxType::Deposit,
            Status::Failure,
            None,
            true,
        ));
        let mut writer = BrokenWriter(Vec::new());
        assert!(
            write_bin(&bin_record, &mut writer)
                .is_err_and(|e| e.to_string() == "Failed to write: test")
        );
        assert!(
            write_bin(&csv_record, &mut writer)
                .is_err_and(|e| e.to_string() == "Failed to write: test")
        );
        assert!(
            write_bin(&txt_record, &mut writer)
                .is_err_and(|e| e.to_string() == "Failed to write: test")
        );
    }

    #[test]
    fn test_write_csv() {
        let bin_record = Record::Bin(create_bin_record(
            TxType::Deposit,
            Status::Failure,
            Some(4),
            true,
        ));
        let csv_record = Record::Csv(create_csv_record(
            TxType::Deposit,
            Status::Failure,
            None,
            true,
        ));
        let txt_record = Record::Txt(create_txt_record(
            TxType::Deposit,
            Status::Failure,
            None,
            true,
        ));
        let mut writer = BrokenWriter(Vec::new());
        assert!(
            write_csv(&bin_record, &mut writer)
                .is_err_and(|e| e.to_string() == "Failed to write: test")
        );
        assert!(
            write_csv(&csv_record, &mut writer)
                .is_err_and(|e| e.to_string() == "Failed to write: test")
        );
        assert!(
            write_csv(&txt_record, &mut writer)
                .is_err_and(|e| e.to_string() == "Failed to write: test")
        );
    }
}
