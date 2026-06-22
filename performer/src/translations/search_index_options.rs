use crate::errors::error::{Error, Result};
use crate::proto::protocol::sdk::search::index_manager;
use couchbase::options::search_index_mgmt_options::{
    AllowQueryingSearchIndexOptions, AnalyzeDocumentOptions, DisallowQueryingSearchIndexOptions,
    DropSearchIndexOptions, FreezePlanSearchIndexOptions, GetAllSearchIndexesOptions,
    GetIndexedDocumentsCountOptions, GetSearchIndexOptions, PauseIngestSearchIndexOptions,
    ResumeIngestSearchIndexOptions, UnfreezePlanSearchIndexOptions, UpsertSearchIndexOptions,
};

impl TryFrom<index_manager::GetSearchIndexOptions> for GetSearchIndexOptions {
    type Error = Box<Error>;

    fn try_from(options: index_manager::GetSearchIndexOptions) -> Result<Self> {
        let opts = GetSearchIndexOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<index_manager::GetAllSearchIndexesOptions> for GetAllSearchIndexesOptions {
    type Error = Box<Error>;

    fn try_from(options: index_manager::GetAllSearchIndexesOptions) -> Result<Self> {
        let opts = GetAllSearchIndexesOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<index_manager::UpsertSearchIndexOptions> for UpsertSearchIndexOptions {
    type Error = Box<Error>;

    fn try_from(options: index_manager::UpsertSearchIndexOptions) -> Result<Self> {
        let opts = UpsertSearchIndexOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<index_manager::DropSearchIndexOptions> for DropSearchIndexOptions {
    type Error = Box<Error>;

    fn try_from(options: index_manager::DropSearchIndexOptions) -> Result<Self> {
        let opts = DropSearchIndexOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<index_manager::GetIndexedSearchIndexOptions> for GetIndexedDocumentsCountOptions {
    type Error = Box<Error>;

    fn try_from(options: index_manager::GetIndexedSearchIndexOptions) -> Result<Self> {
        let opts = GetIndexedDocumentsCountOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<index_manager::PauseIngestSearchIndexOptions> for PauseIngestSearchIndexOptions {
    type Error = Box<Error>;

    fn try_from(options: index_manager::PauseIngestSearchIndexOptions) -> Result<Self> {
        let opts = PauseIngestSearchIndexOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<index_manager::ResumeIngestSearchIndexOptions> for ResumeIngestSearchIndexOptions {
    type Error = Box<Error>;

    fn try_from(options: index_manager::ResumeIngestSearchIndexOptions) -> Result<Self> {
        let opts = ResumeIngestSearchIndexOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<index_manager::AllowQueryingSearchIndexOptions> for AllowQueryingSearchIndexOptions {
    type Error = Box<Error>;

    fn try_from(options: index_manager::AllowQueryingSearchIndexOptions) -> Result<Self> {
        let opts = AllowQueryingSearchIndexOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<index_manager::DisallowQueryingSearchIndexOptions>
    for DisallowQueryingSearchIndexOptions
{
    type Error = Box<Error>;

    fn try_from(options: index_manager::DisallowQueryingSearchIndexOptions) -> Result<Self> {
        let opts = DisallowQueryingSearchIndexOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<index_manager::FreezePlanSearchIndexOptions> for FreezePlanSearchIndexOptions {
    type Error = Box<Error>;

    fn try_from(options: index_manager::FreezePlanSearchIndexOptions) -> Result<Self> {
        let opts = FreezePlanSearchIndexOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<index_manager::UnfreezePlanSearchIndexOptions> for UnfreezePlanSearchIndexOptions {
    type Error = Box<Error>;

    fn try_from(options: index_manager::UnfreezePlanSearchIndexOptions) -> Result<Self> {
        let opts = UnfreezePlanSearchIndexOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}

impl TryFrom<index_manager::AnalyzeDocumentOptions> for AnalyzeDocumentOptions {
    type Error = Box<Error>;

    fn try_from(options: index_manager::AnalyzeDocumentOptions) -> Result<Self> {
        let opts = AnalyzeDocumentOptions::default();

        if let Some(_timeout_msecs) = options.timeout_msecs {
            return Err(Error::unimplemented("timeout_msecs is unimplemented"));
        }

        Ok(opts)
    }
}
