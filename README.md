# Storage Fee Manager

This is a command-line based simulator for managing cloud storage operations and calculating usage fees under different plan constraints.

It supports the following operations:
- `UPLOAD`: Upload a file to a storage unit
- `DELETE`: Delete a file from a storage unit
- `UPDATE`: Update an existing file's size
- `CALC`: Calculate monthly billing statistics

### Features
- Models storage units with customizable per-MB storage/update fees
- Enforces free plan limitations with monthly fee caps
- Tracks max monthly usage and update volume per unit
- Outputs total fees (storage, update, and usage) for each operation
- Implements input validation and month-based statistics

### Technologies Used
- Language: Python 3
- Design: Object-oriented with `@dataclass`
- No external dependencies

### Example Command

```text
2060-04-01T00:00 UPLOAD storage_A1 file123 5000
