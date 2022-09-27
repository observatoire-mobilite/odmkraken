import pytest
from odmkraken.busspeeds.extract import *
from pathlib import Path
import dagster


def test_filealreadyimportederror():
    file = Path('test.file')
    checksum = b'moin'
    err = FileAlreadyImportedError(file, checksum)
    assert err.file == file
    assert err.checksum == checksum 
    assert str(file) in str(err)


def test_extract_from_csv(mocker):
    fake_edmo = mocker.patch('odmkraken.resources.edmo.busdata.EDMOVehData', autospec=True)
    fake_of = mocker.patch('odmkraken.busspeeds.extract.open_file')
    fake_handle = mocker.Mock()
    fake_of.return_value = 10000, fake_handle
    fake_cc = mocker.patch('odmkraken.busspeeds.extract.compute_checksum')
    fake_cc.return_value = b'moin'
    fake_if = mocker.patch('odmkraken.busspeeds.extract.infer_format')
    fake_if.return_value = {'sep': '?', 'date': 'DATE'}
    fake_file = '/some/where/a/file.csv.zip'
    fake_uuid = mocker.patch('uuid.uuid4')
    fake_uuid.return_value='abcdefgh'
    
    ctx = dagster.build_op_context(
        resources={'edmo_vehdata': fake_edmo},
        config={'file': '/some/where/a/file.csv.zip'}
    )
    
    fake_edmo.check_file_already_imported.return_value = False
    extract_from_csv(ctx)

    fake_of.assert_called_once_with(Path(fake_file))
    fake_if.assert_called_once_with(fake_handle)
    fake_cc.assert_called_once_with(fake_handle)
    fake_edmo.check_file_already_imported.assert_called_once_with(b'moin')
    fake_edmo.import_csv_file.assert_called_once_with(fake_handle, sep='?', table='_import_abcdefgh')

    fake_of.return_value = 0, fake_handle
    extract_from_csv(ctx)

    fake_edmo.check_file_already_imported.return_value = True
    with pytest.raises(FileAlreadyImportedError):
        extract_from_csv(ctx)


def test_unusablezipfile():
    file = Path('test.file')
    n_files = 10
    err = UnusableZipFile(file, n_files=n_files)
    assert err.file == file
    assert err.n_files == n_files 
    assert str(file) in str(err)
    assert str(n_files) in str(err)


def test_open_file(mocker):
    fake_file = mocker.Mock()
    fake_iszip = mocker.patch('zipfile.is_zipfile')
    fake_zip = mocker.patch('zipfile.ZipFile')
    fake_zipfile = mocker.Mock(spec=['filelist', 'open'])
    fake_zippedfile = mocker.Mock(spec=['file_size'])
    fake_zippedfile.file_size = 100
    fake_zipfile.filelist = [fake_zippedfile]
    fake_zip.return_value = fake_zipfile

    # called on a non-zip file
    fake_iszip.return_value = False
    nbytes, handle = open_file(fake_file)
    fake_iszip.assert_called_once_with(fake_file)
    assert nbytes == 0
    assert handle == fake_file.open('rb')
    
    # called on a zip-file
    fake_iszip.return_value = True
    nbytes, handle = open_file(fake_file)
    fake_zip.assert_called_once_with(fake_file)
    assert nbytes == 100
    assert handle == fake_zipfile.open(fake_zippedfile, 'r')

    # called on invalid zip-files
    for i in (0, 20):
        fake_zipfile.filelist = [mocker.Mock()] * i
        with pytest.raises(UnusableZipFile):
            open_file(fake_file)


def test_human_readable_bytes():
    for bts, hmn in ((-1000, '0 bytes'), (100, '100 bytes'),
                     (1001, '1.00 kB'), (1000000, '1.00 MB'),
                     (2010000, '2.01 MB'), (1000000000, '1.00 GB'),
                     (4199000000000, '4.20 TB')):
        assert human_readable_bytes(bts) == hmn
    

def test_headerlacksfield():
    err = HeaderLacksField(('test', ))
    assert 'test' in str(err)
    assert err.fields == ['test']
    assert isinstance(err, CSVFormatProblem)


def test_unknowncsvdialect():
    err = UnkownCSVDialect()
    assert 'file is not in CSV format' in str(err)
    assert isinstance(err, CSVFormatProblem)


def test_unsupporteddateformat():
    err = UnsupportedDateFormat('test')
    assert err.date == 'test'
    assert 'test' in str(err)
    assert isinstance(err, CSVFormatProblem)


def test_infer_format(mocker):
    fake_handle = mocker.Mock(spec=['readline'])

    # just the wrong file
    fake_handle.readline.side_effect = iter([b'moin', b'ca', b'va?'])
    with pytest.raises(UnkownCSVDialect):
        infer_format(fake_handle)

    # incomplete and false headers
    fake_header = (', '.join(list(CSV_REQUIRED_FIELDS)[:6])).encode('utf-8')
    fake_handle.readline.side_effect = iter([fake_header + b', FAKE', b'', b''])
    with pytest.raises(HeaderLacksField) as info:
        infer_format(fake_handle)
    assert set(info.value.fields) == set(list(CSV_REQUIRED_FIELDS)[6:])

    # wrong date format
    fake_header = ','.join(CSV_REQUIRED_FIELDS).encode('utf-8')
    fake_lines = ['nothing'] * len(CSV_REQUIRED_FIELDS)
    fake_lines = ','.join(fake_lines).encode('utf-8')
    fake_handle.readline.side_effect = iter([fake_header, fake_lines, fake_lines])
    with pytest.raises(UnsupportedDateFormat) as info:
        infer_format(fake_handle)
    assert info.value.date == 'nothing'

    # a valid file
    datetypes = {
        'DD.MM.YYYY': '04.12.2022',
        'DD-MON-YY': '04-DEC-22',
        'DD-Mon-YY': '04-Dec-22',
        'DD-MON-YYYY': '04-DEC-2022',
        'DD-Mon-YYYY': '04-Dec-2022',
        'YYYY-MM-DD': '2022-12-04'
    }
    for fmt_str, date in datetypes.items():
        # TODO: replace by more realistic test (not all fields == date)
        fake_lines = ['nothing'] * len(CSV_REQUIRED_FIELDS)
        fake_lines = [date] * len(CSV_REQUIRED_FIELDS)
        fake_lines = ','.join(fake_lines).encode('utf-8')
        fake_handle.readline.side_effect = iter([fake_header, fake_lines, fake_lines])
        format = infer_format(fake_handle)
        assert format['sep'] == ','
        assert format['date'] == fmt_str
    

def test_infer_format_on_completely_empty_file(mocker):
    # mimmick a complete
    fake_handle = mocker.Mock(spec=['readline'])
    fake_handle.readline.side_effect = iter([b'', b'', b''])
    with pytest.raises(FileIsEmpty):
        infer_format(fake_handle)


def test_infer_format_on_correct_but_empty_file(mocker):
    # mimmick a correctly formed but otherwise empty file
    fake_handle = mocker.Mock(spec=['readline'])
    fake_header = (', '.join(list(CSV_REQUIRED_FIELDS))).encode('utf-8')
    fake_handle.readline.side_effect = iter([fake_header, b'', b''])
    with pytest.raises(FileHasNoData):
        infer_format(fake_handle)
    

def test_compute_checksum(mocker):
    fake_handle = mocker.Mock(spec=['seek', 'read'])
    chunk = b'abcdefghijklmnopqrstuvwxyzABCD'
    fake_handle.read.side_effect = (chunk if i < 99 else None for i in range(100))
    fake_hash = mocker.patch('hashlib.sha256', spec=['update', 'digest'])
    result = compute_checksum(fake_handle, file_hash=fake_hash, chunk_size=len(chunk))
    fake_hash.update.assert_called_with(chunk)
    assert fake_hash.update.call_count == 99
    fake_handle.seek.assert_called_with(0)
    assert fake_handle.seek.call_count == 2
    fake_hash.digest.assert_called_once_with()


