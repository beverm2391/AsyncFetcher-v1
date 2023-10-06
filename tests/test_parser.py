import pytest
import asyncio

from lib.fetcher import ResponseParser

class FakeResponse:
    def __init__(self, data, content_type):
        self.data = data
        self.headers = {'Content-Type': content_type}

    async def json(self):
        return self.data
    
    async def text(self):
        return self.data

# ! Fixtures =================================================
@pytest.fixture
def json_response():
    data = {"key": "value"}  # Replace with the data you expect to test
    return FakeResponse(data, 'application/json')

@pytest.fixture
def html_response():
    data = "<html><body>Hello, world!</body></html>"
    return FakeResponse(data, 'text/html')

@pytest.fixture
def xml_response():
    data = "<note><to>Tove</to><from>Jani</from></note>"
    return FakeResponse(data, 'application/xml')

@pytest.fixture
def text_response():
    data = "Hello, world!"
    return FakeResponse(data, 'text/plain')

@pytest.fixture
def invalid_content_type_response():
    data = "Invalid content type"
    return FakeResponse(data, 'invalid/type')

# ! Tests ====================================================
@pytest.mark.asyncio
async def test_json(json_response): 
    parser = ResponseParser()

    result = await parser.parse(json_response)
    assert result == {"key": "value"}, f"Expected {json_response.data}, got {result}"

@pytest.mark.asyncio
async def test_html(html_response): 
    parser = ResponseParser()

    result = await parser.parse(html_response)
    assert result == "<html><body>Hello, world!</body></html>", f"Expected {html_response.data}, got {result}"

@pytest.mark.asyncio
async def test_xml(xml_response): 
    parser = ResponseParser()

    result = await parser.parse(xml_response)
    assert result == "<note><to>Tove</to><from>Jani</from></note>", f"Expected {xml_response.data}, got {result}"

@pytest.mark.asyncio
async def test_text(text_response): 
    parser = ResponseParser()

    result = await parser.parse(text_response)
    assert result == "Hello, world!", f"Expected {text_response.data}, got {result}"

@pytest.mark.asyncio
async def test_invalid_content_type(invalid_content_type_response): 
    parser = ResponseParser()

    with pytest.raises(ValueError):
        await parser.parse(invalid_content_type_response)