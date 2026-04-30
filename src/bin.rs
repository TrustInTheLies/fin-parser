use std::io::Read;

use crate::{
    Amount, Body, DescLen, Description, FromUserId, Head, ReadError, Record, Status, Timestamp,
    ToUserId, TxId, TxType, WriteError,
};

/// Header described in format schema
pub(crate) const MAGIC: [u8; 4] = [0x59, 0x50, 0x42, 0x4E];
/// Size of all fields combined without description, cause it's dynamic
pub(crate) const SIZE_WITHOUT_DESCRIPTION: u32 = 46;

#[derive(Debug, Clone)]
pub struct YPBankBinRecord {
    #[allow(unused)] // just to suppress warning, field is actually used
    pub(crate) head: Head,
    pub(crate) body: Body,
}

impl YPBankBinRecord {
    pub(crate) fn parse<R: Read>(mut reader: R) -> Result<Vec<Record>, ReadError> {
        let mut data = Vec::new();
        let mut records = Vec::new();
        reader
            .read_to_end(&mut data)
            .map_err(|e| ReadError::FailedReader(format!("Failed to read: {}", e)))?;
        while !data.is_empty() {
            let head = Self::parse_head(&mut data)?;
            let mut payload = data.drain(..head.record_size as usize).collect::<Vec<u8>>();
            let tx_id = Self::parse_tx_id(&mut payload)?;
            let tx_type = Self::parse_tx_type(&mut payload)?;
            let from_user_id = Self::parse_from_user_id(&mut payload, &tx_type)?;
            let to_user_id = Self::parse_to_user_id(&mut payload, &tx_type)?;
            let amount = Self::parse_amount(&mut payload)?;
            let timestamp = Self::parse_timestamp(&mut payload)?;
            let status = Self::parse_status(&mut payload)?;
            let desc_len = Self::parse_desc_len(&mut payload)?;
            let description = Self::parse_description(&mut payload, &desc_len)?;
            let body = Body(
                tx_id,
                tx_type,
                from_user_id,
                to_user_id,
                amount,
                timestamp,
                status,
                desc_len,
                description,
            );
            let record = Record::Bin(YPBankBinRecord { head, body });
            records.push(record);
        }
        Ok(records)
    }

    fn parse_head(data: &mut Vec<u8>) -> Result<Head, ReadError> {
        if data.len() < 8 {
            return Err(ReadError::MismatchedSize(
                "Provided binary isn't complete".to_string(),
            ));
        }
        let head = data.drain(..8).collect::<Vec<u8>>();
        let (magic, record_size) = head.split_at(4);
        let magic: [u8; 4] = magic.try_into()?;
        if magic != MAGIC {
            println!(
                "expected {:?}, got {:?}, size: {:?}",
                MAGIC, magic, record_size
            );
            return Err(ReadError::IncorrectData("Incorrect header".into()));
        };
        let record_size: [u8; 4] = record_size.try_into()?;
        let magic = u32::from_be_bytes(magic);
        let record_size = u32::from_be_bytes(record_size);
        let head = Head { magic, record_size };
        Ok(head)
    }

    fn parse_tx_id(payload: &mut Vec<u8>) -> Result<TxId, ReadError> {
        if payload.len() < 8 {
            return Err(ReadError::MismatchedSize(
                "Provided binary isn't complete".to_string(),
            ));
        }
        let tx_id = payload.drain(..8).collect::<Vec<u8>>();
        let tx_id: [u8; 8] = tx_id.as_slice().try_into()?;
        let tx_id = u64::from_be_bytes(tx_id);
        Ok(TxId(tx_id))
    }

    fn parse_tx_type(payload: &mut Vec<u8>) -> Result<TxType, ReadError> {
        if payload.is_empty() {
            return Err(ReadError::MismatchedSize(
                "Provided binary isn't complete".to_string(),
            ));
        }
        let tx_type = payload.drain(..1).collect::<Vec<u8>>();
        match tx_type[0] {
            0 => Ok(TxType::Deposit),
            1 => Ok(TxType::Transfer),
            2 => Ok(TxType::Withdrawal),
            _ => Err(ReadError::IncorrectData("Incorrect tx type".into())),
        }
    }

    fn parse_from_user_id(
        payload: &mut Vec<u8>,
        tx_type: &TxType,
    ) -> Result<FromUserId, ReadError> {
        if payload.len() < 8 {
            return Err(ReadError::MismatchedSize(
                "Provided binary isn't complete".to_string(),
            ));
        }
        let from_user_id = payload.drain(..8).collect::<Vec<u8>>();
        match tx_type {
            TxType::Deposit => Ok(FromUserId(0)),
            _ => {
                let from_user_id: [u8; 8] = from_user_id.as_slice().try_into()?;
                let from_iser_id = u64::from_be_bytes(from_user_id);
                Ok(FromUserId(from_iser_id))
            }
        }
    }

    fn parse_to_user_id(payload: &mut Vec<u8>, tx_type: &TxType) -> Result<ToUserId, ReadError> {
        if payload.len() < 8 {
            return Err(ReadError::MismatchedSize(
                "Provided binary isn't complete".to_string(),
            ));
        }
        let to_user_id = payload.drain(..8).collect::<Vec<u8>>();
        match tx_type {
            TxType::Withdrawal => Ok(ToUserId(0)),
            _ => {
                let to_user_id: [u8; 8] = to_user_id.as_slice().try_into()?;
                let to_iser_id = u64::from_be_bytes(to_user_id);
                Ok(ToUserId(to_iser_id))
            }
        }
    }

    fn parse_amount(payload: &mut Vec<u8>) -> Result<Amount, ReadError> {
        if payload.len() < 8 {
            return Err(ReadError::MismatchedSize(
                "Provided binary isn't complete".to_string(),
            ));
        }
        let amount = payload.drain(..8).collect::<Vec<u8>>();
        let amount: [u8; 8] = amount.as_slice().try_into()?;
        let amount = u64::from_be_bytes(amount);
        Ok(Amount(amount))
    }

    fn parse_timestamp(payload: &mut Vec<u8>) -> Result<Timestamp, ReadError> {
        if payload.len() < 8 {
            return Err(ReadError::MismatchedSize(
                "Provided binary isn't complete".to_string(),
            ));
        }
        let timestamp = payload.drain(..8).collect::<Vec<u8>>();
        let timestamp: [u8; 8] = timestamp.as_slice().try_into()?;
        let timestamp = u64::from_be_bytes(timestamp);
        Ok(Timestamp(timestamp))
    }

    fn parse_status(payload: &mut Vec<u8>) -> Result<Status, ReadError> {
        if payload.is_empty() {
            return Err(ReadError::MismatchedSize(
                "Provided binary isn't complete".to_string(),
            ));
        }
        let status = payload.drain(..1).collect::<Vec<u8>>();
        match status[0] {
            0 => Ok(Status::Success),
            1 => Ok(Status::Failure),
            2 => Ok(Status::Pending),
            _ => Err(ReadError::IncorrectData("Incorrect status".into())),
        }
    }

    fn parse_desc_len(payload: &mut Vec<u8>) -> Result<DescLen, ReadError> {
        if payload.len() < 4 {
            return Err(ReadError::MismatchedSize(
                "Provided binary isn't complete".to_string(),
            ));
        }
        let desc_len = payload.drain(..4).collect::<Vec<u8>>();
        let desc_len: [u8; 4] = desc_len.as_slice().try_into()?;
        let desc_len = u32::from_be_bytes(desc_len);
        Ok(DescLen(Some(desc_len)))
    }

    fn parse_description(
        payload: &mut Vec<u8>,
        desc_len: &DescLen,
    ) -> Result<Description, ReadError> {
        if let Some(len) = desc_len.0 {
            if payload.len() < len as usize {
                return Err(ReadError::MismatchedSize(
                    "Provided binary isn't complete".to_string(),
                ));
            }
            match len {
                0 => Ok(Description(None)),
                _ => {
                    let description = payload.drain(..len as usize).collect::<Vec<u8>>();
                    Ok(Description(Some(description)))
                }
            }
        } else {
            Err(ReadError::IncorrectData("Incorrect description".into()))
        }
    }

    pub(crate) fn build_data(&self) -> Result<Vec<u8>, WriteError> {
        let record_size = SIZE_WITHOUT_DESCRIPTION + self.body.7.0.unwrap_or(0);
        let head = Head {
            magic: u32::from_be_bytes(MAGIC),
            record_size,
        };
        let mut data = Vec::from([
            head.magic.to_be_bytes().to_vec(),
            head.record_size.to_be_bytes().to_vec(),
            self.body.0.0.to_be_bytes().to_vec(),
            self.write_tx_type(),
            self.body.2.0.to_be_bytes().to_vec(),
            self.body.3.0.to_be_bytes().to_vec(),
            self.body.4.0.to_be_bytes().to_vec(),
            self.body.5.0.to_be_bytes().to_vec(),
            self.write_status(),
            self.write_desc_len()?,
            // self.body.8.0.to_be_bytes().to_vec(),
        ]);
        if let Some(v) = &self.body.8.0 {
            if v.len() as u32 > u32::MAX {
                return Err(WriteError::IncorrectData(
                    "DESCRIPTION value overflows max size".to_string(),
                ));
            }
            data.push(v.to_owned());
        };

        Ok(data.iter().flat_map(|f| f.to_owned()).collect::<Vec<u8>>())
    }

    fn write_tx_type(&self) -> Vec<u8> {
        match self.body.1 {
            TxType::Deposit => 0_u8.to_be_bytes().to_vec(),
            TxType::Transfer => 1_u8.to_be_bytes().to_vec(),
            TxType::Withdrawal => 2_u8.to_be_bytes().to_vec(),
        }
    }

    fn write_status(&self) -> Vec<u8> {
        match self.body.6 {
            Status::Success => 0_u8.to_be_bytes().to_vec(),
            Status::Failure => 1_u8.to_be_bytes().to_vec(),
            Status::Pending => 2_u8.to_be_bytes().to_vec(),
        }
    }

    fn write_desc_len(&self) -> Result<Vec<u8>, WriteError> {
        if let Some(v) = self.body.7.0
            && self
                .body
                .8
                .0
                .as_ref()
                .is_some_and(|len| len.len() as u32 != v)
        {
            return Err(WriteError::IncorrectData(
                "DESC_LEN value and DESCRIPTION size does not match".into(),
            ));
        }
        if self.body.7.0.is_none() && self.body.8.0.is_some() {
            return Err(WriteError::IncorrectData(
                "DESC_LEN value and DESCRIPTION size does not match".into(),
            ));
        }
        match self.body.7.0 {
            Some(v) => Ok(v.to_be_bytes().to_vec()),
            None => Ok(0_u32.to_be_bytes().to_vec()),
        }
    }
}

mod tests {
    use crate::{
        Amount, Body, DescLen, Description, FromUserId, Head, Status, Timestamp, ToUserId, TxId,
        TxType,
        bin::{MAGIC, SIZE_WITHOUT_DESCRIPTION, YPBankBinRecord},
    };

    fn create_head() -> Head {
        let record_size = SIZE_WITHOUT_DESCRIPTION + 3;
        Head {
            magic: u32::from_be_bytes(MAGIC),
            record_size,
        }
    }

    fn create_body(txtype: TxType, status: Status, size: Option<u32>) -> Body {
        Body(
            TxId(1),
            txtype,
            FromUserId(0),
            ToUserId(1),
            Amount(1),
            Timestamp(1),
            status,
            DescLen(size),
            Description(Some(Vec::from("bye"))),
        )
    }

    fn create_bin_record(txtype: TxType, status: Status, size: Option<u32>) -> YPBankBinRecord {
        YPBankBinRecord {
            head: create_head(),
            body: create_body(txtype, status, size),
        }
    }

    #[test]
    fn test_write_tx_type() {
        let data = create_bin_record(TxType::Deposit, Status::Failure, Some(3));
        assert_eq!(data.write_tx_type(), [0]);
        let data = create_bin_record(TxType::Transfer, Status::Failure, Some(3));
        assert_eq!(data.write_tx_type(), [1]);
        let data = create_bin_record(TxType::Withdrawal, Status::Failure, Some(3));
        assert_eq!(data.write_tx_type(), [2]);
    }

    #[test]
    fn test_write_status() {
        let data = create_bin_record(TxType::Deposit, Status::Failure, Some(3));
        assert_eq!(data.write_status(), [1]);
        let data = create_bin_record(TxType::Deposit, Status::Success, Some(3));
        assert_eq!(data.write_status(), [0]);
        let data = create_bin_record(TxType::Deposit, Status::Pending, Some(3));
        assert_eq!(data.write_status(), [2]);
    }

    #[test]
    fn test_write_description() {
        // since DESC_LEN and DESCRIPTION are dependant on each other, we'll juggle values
        let data = create_bin_record(TxType::Deposit, Status::Failure, Some(2));
        assert!(data.write_desc_len().is_err());
        let data = create_bin_record(TxType::Deposit, Status::Failure, None);
        assert!(data.write_desc_len().is_err());
        let data = create_bin_record(TxType::Deposit, Status::Failure, Some(3));
        assert!(data.write_desc_len().is_ok());
    }
}
