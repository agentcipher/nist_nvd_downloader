from datetime import date, datetime, timezone
from typing import Dict, Any, List, Iterator
import json
from pydantic import BaseModel, Field

class NVD(BaseModel):
    cve_id: str = Field(primary_key=True)
    data: str
    first_seen: datetime
    last_modified: datetime
    ingest_ts: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Ingestion timestamp"
    )

class Response(BaseModel):
    data: List[Dict[str, Any]]

    def get_data(self) -> Iterator[Dict[str, Any]]:
        for rows in self.data:
            row = NVD(
                cve_id=rows.get('cve', {}).get('id', ''),
                data=json.dumps(rows.get('cve', {})),
                first_seen=rows.get('cve', {}).get('published'),  # Keep it as string for now
                last_modified=rows.get('cve', {}).get('lastModified')  # Keep it as string for now
            )
            yield row

def parse_data(raw_data: Dict[str, Any]) -> Response:
    return Response(**raw_data)
