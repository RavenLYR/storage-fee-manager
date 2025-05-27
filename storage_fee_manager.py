"""
Storage Fee Manager - A system to manage storage fees for different storage units.

This module implements a storage fee management system that:
1. Handles file operations (upload, delete, update) across different storage units
2. Calculates storage and update fees based on file sizes
3. Enforces free plan limitations
4. Maintains monthly statistics for billing
"""

from datetime import datetime, timedelta
from decimal import Decimal
import math
from typing import Dict, Tuple 
from dataclasses import dataclass, field 
from collections import defaultdict
import sys

@dataclass(frozen=True)
class StorageUnit:
    """
    Represents a storage unit with its fee structure and plan availability.
    
    Attributes:
        type: Storage type identifier ('A' or 'B')
        store_fee_per_mb: Fee charged per MB for storage
        update_fee_per_mb: Fee charged per MB for update operations
        is_free_plan_allowed: Whether this storage unit is available on free plan
    """
    type: str
    store_fee_per_mb: Decimal
    update_fee_per_mb: Decimal
    is_free_plan_allowed: bool

# Storage unit configurations with their respective fee structures
STORAGE_UNITS = {
    'storage_A1': StorageUnit('A', Decimal('0.01'), Decimal('0.0005'), True),
    'storage_A2': StorageUnit('A', Decimal('0.001'), Decimal('0.01'), True),
    'storage_B1': StorageUnit('B', Decimal('0.01'), Decimal('0.001'), False),
    'storage_B2': StorageUnit('B', Decimal('0.0001'), Decimal('0.5'), False)
}

@dataclass
class File:
    """
    Represents a file in the storage system.
    
    Attributes:
        name: Unique identifier for the file
        size: Size of the file in KB
        storage: Name of the storage unit containing the file
    """
    name: str
    size: int
    storage: str

@dataclass
class MonthlyStats:
    """
    Tracks monthly statistics for a storage unit.
    
    Attributes:
        max_size: Maximum storage size used in the month (in KB)
        update_kb_sum: Total size of update operations in the month (in KB)
    """
    max_size: int = 0 
    update_kb_sum: int = 0

class StorageManager:
    """
    Manages storage operations and fee calculations.
    
    This class handles all storage operations (upload, delete, update),
    tracks storage usage, and calculates fees based on the storage unit's
    fee structure and the user's plan type (free or paid).
    """
    
    def __init__(self, is_free_plan: bool = True):
        """
        Initialize the storage manager.
        
        Args:
            is_free_plan: Whether this instance is operating under free plan limitations
        """
        self.is_free_plan = is_free_plan
        self.files: Dict[str, File] = {}
        self.current_storage_size: Dict[str, int] = defaultdict(int)
        self.monthly_stats: Dict[str, Dict[str, MonthlyStats]] = {}
        self.calc_reported_sizes_snapshot: Dict[str, Dict[str, int]] = {}
        self.KB_TO_MB_DIVISOR = 1000
        self.update_fee_settled_for_month: set[str] = set()
        # Stores the actual storage sizes at the end of each month for initialization
        self.last_month_eom_storage_sizes: Dict[str, int] = defaultdict(int)

    def _get_month_key(self, timestamp: datetime) -> str:
        """
        Generate a key for monthly statistics based on timestamp.
        Format: YYYY-MM
        """
        return f"{timestamp.year}-{timestamp.month:02d}"

    def _get_previous_month_key(self, timestamp: datetime) -> str:
        """
        Generate a key for the previous month's statistics.
        Used for CALC operations which report on the previous month.
        """
        current_month_start = timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        prev_month_end = current_month_start - timedelta(days=1)
        return self._get_month_key(prev_month_end)

    def _ensure_month_init(self, month_key: str):
        """
        Initialize monthly statistics if not already present.
        Inherits actual storage sizes from the previous month.
        """
        if month_key not in self.monthly_stats:
            self.monthly_stats[month_key] = defaultdict(MonthlyStats)
            # Initialize new month's max_size with actual storage sizes from previous month
            for s_name, eom_size in self.last_month_eom_storage_sizes.items():
                self.monthly_stats[month_key][s_name].max_size = eom_size

    def _would_exceed_free_plan_limit(self, month_key: str, 
                                       op_storage_name: str, 
                                       op_potential_max_size_kb: int, 
                                       op_kb_for_current_op: int 
                                       ) -> bool:
        """
        Check if an operation would exceed the free plan's fee limit.
        
        This method simulates the operation and calculates the total integer fees
        that would result. The free plan limit is exceeded if the sum of integer
        storage and update fees would exceed 1000.
        
        Args:
            month_key: The month being checked
            op_storage_name: The storage unit for the operation
            op_potential_max_size_kb: Potential new max size after operation
            op_kb_for_current_op: Size impact of the current operation
            
        Returns:
            bool: True if the operation would exceed the free plan limit
        """
        if not self.is_free_plan:
            return False

        sim_total_integer_storage_fee = 0
        sim_total_integer_update_fee = 0
        
        self._ensure_month_init(month_key) 
        current_month_data = self.monthly_stats[month_key]

        for s_name, s_unit in STORAGE_UNITS.items():
            if not s_unit.is_free_plan_allowed:
                continue
            
            s_stats = current_month_data[s_name] 
            
            # Simulate storage and update amounts after operation
            current_s_max_size_kb_for_sim = s_stats.max_size
            current_s_update_kb_sum_for_sim = s_stats.update_kb_sum

            if s_name == op_storage_name:
                current_s_max_size_kb_for_sim = op_potential_max_size_kb
                current_s_update_kb_sum_for_sim += op_kb_for_current_op
            
            # Calculate integer storage fee
            if current_s_max_size_kb_for_sim > 0:
                s_mb_for_storage = math.ceil(current_s_max_size_kb_for_sim / self.KB_TO_MB_DIVISOR)
                storage_fee_dec_item = Decimal(s_mb_for_storage) * s_unit.store_fee_per_mb
                ceiled_storage_fee_item = math.ceil(storage_fee_dec_item)
                sim_total_integer_storage_fee += ceiled_storage_fee_item
            
            # Calculate integer update fee
            if current_s_update_kb_sum_for_sim > 0:
                u_mb_for_fee = math.ceil(current_s_update_kb_sum_for_sim / self.KB_TO_MB_DIVISOR)
                update_fee_dec_item = Decimal(u_mb_for_fee) * s_unit.update_fee_per_mb
                ceiled_update_fee_item = math.ceil(update_fee_dec_item)
                sim_total_integer_update_fee += ceiled_update_fee_item
        
        return (sim_total_integer_storage_fee + sim_total_integer_update_fee) > 1000

    def _calculate_total_fees(self, month_key: str) -> Dict[str, int]:
        """
        Calculate total fees for a given month.
        
        Calculates storage fees based on maximum storage used and
        update fees based on total update operations. For free plan,
        only considers allowed storage units.
        
        Returns:
            Dict containing storage_fee, update_fee, and usage_fee
        """
        self._ensure_month_init(month_key)
        final_total_storage_fee, final_total_update_fee = 0, 0
        total_storage_fee_dec_for_usage, total_update_fee_dec_for_usage = Decimal(0), Decimal(0)
        
        # Only consider allowed storage units for free plan
        storages_to_check = [s_name for s_name, s_unit in STORAGE_UNITS.items()
                             if not self.is_free_plan or s_unit.is_free_plan_allowed]
        current_month_data = self.monthly_stats[month_key]

        for s_name in storages_to_check:
            s_unit = STORAGE_UNITS[s_name]
            s_stats = current_month_data[s_name] 

            # Calculate storage fee based on max size
            if s_stats.max_size > 0:
                s_mb = math.ceil(s_stats.max_size / self.KB_TO_MB_DIVISOR)
                storage_fee_dec_item = Decimal(s_mb) * s_unit.store_fee_per_mb
                final_total_storage_fee += math.ceil(storage_fee_dec_item)
                total_storage_fee_dec_for_usage += storage_fee_dec_item

            # Calculate update fee if not already settled
            current_s_update_kb_sum_for_calc = s_stats.update_kb_sum
            if month_key in self.update_fee_settled_for_month:
                 current_s_update_kb_sum_for_calc = 0

            if current_s_update_kb_sum_for_calc > 0:
                u_mb = math.ceil(current_s_update_kb_sum_for_calc / self.KB_TO_MB_DIVISOR)
                update_fee_dec_item = Decimal(u_mb) * s_unit.update_fee_per_mb
                final_total_update_fee += math.ceil(update_fee_dec_item)

            # Track update fees for usage calculation
            if month_key in self.update_fee_settled_for_month: 
                total_update_fee_dec_for_usage += Decimal(0)
            elif s_stats.update_kb_sum > 0:
                 u_mb_for_usage = math.ceil(s_stats.update_kb_sum / self.KB_TO_MB_DIVISOR)
                 total_update_fee_dec_for_usage += Decimal(u_mb_for_usage) * s_unit.update_fee_per_mb
                 
        # Calculate usage fee for free plan (amount exceeding 1000)
        usage_fee = math.ceil(max(Decimal(0), total_storage_fee_dec_for_usage + total_update_fee_dec_for_usage - Decimal(1000)))
        return {
            "storage_fee": final_total_storage_fee,
            "update_fee": final_total_update_fee,
            "usage_fee": usage_fee,
        }

    def handle_upload(self, timestamp: datetime, storage_name: str, file_name: str, size: int) -> str:
        """
        Handle file upload operation.
        
        Validates the operation against storage availability and free plan limits.
        Updates storage statistics and calculates fees.
        
        Returns:
            Status message with operation result and fees if successful
        """
        if file_name in self.files: return "UPLOAD: file already exists"
        if storage_name not in STORAGE_UNITS: return "UPLOAD: invalid storage name"
        unit = STORAGE_UNITS[storage_name]
        if self.is_free_plan and not unit.is_free_plan_allowed:
            return "UPLOAD: this storage location is not available on the free plan"
        
        month_key = self._get_month_key(timestamp)
        self._ensure_month_init(month_key) 

        # Check free plan limits
        if self.is_free_plan:
            current_stats_for_op_storage = self.monthly_stats[month_key][storage_name]
            simulated_current_total_size_for_op_storage = self.current_storage_size.get(storage_name, 0) + size
            potential_max_size_kb = max(current_stats_for_op_storage.max_size, simulated_current_total_size_for_op_storage)
            
            if self._would_exceed_free_plan_limit(month_key, storage_name, 
                                                 op_potential_max_size_kb=potential_max_size_kb, 
                                                 op_kb_for_current_op=size):
                return "UPLOAD: free plan fee limit exceeded"
        
        # Perform upload and update statistics
        self.files[file_name] = File(file_name, size, storage_name)
        self.current_storage_size[storage_name] += size
        stats_to_update = self.monthly_stats[month_key][storage_name]
        stats_to_update.max_size = max(stats_to_update.max_size, self.current_storage_size[storage_name])
        stats_to_update.update_kb_sum += size 
        
        total_fees = self._calculate_total_fees(month_key)
        return f"UPLOAD: {total_fees['storage_fee']} {total_fees['update_fee']} {total_fees['usage_fee']}"

    def handle_delete(self, timestamp: datetime, storage_name: str, file_name: str) -> str:
        """
        Handle file deletion operation.
        
        Validates the operation against storage availability and free plan limits.
        Updates storage statistics and calculates fees.
        
        Returns:
            Status message with operation result and fees if successful
        """
        month_key = self._get_month_key(timestamp) 
        self._ensure_month_init(month_key) 
        if storage_name not in STORAGE_UNITS:
             return f"DELETE: invalid storage name" 
        unit = STORAGE_UNITS[storage_name]
        if self.is_free_plan and not unit.is_free_plan_allowed:
            return "DELETE: this storage location is not available on the free plan"
        if file_name not in self.files: 
            return "DELETE: file does not exist" 
        file = self.files[file_name]
        if file.storage != storage_name: 
            return "DELETE: file is not in the specified storage" 
        deleted_file_size = file.size 
        
        # Check free plan limits
        if self.is_free_plan:
            current_stats_for_op_storage = self.monthly_stats[month_key][storage_name]
            # Delete operation doesn't increase max_size
            potential_max_size_kb = current_stats_for_op_storage.max_size
            # Update amount for delete is the size of deleted file
            if self._would_exceed_free_plan_limit(month_key, storage_name,
                                                 op_potential_max_size_kb=potential_max_size_kb, 
                                                 op_kb_for_current_op=deleted_file_size):
                return "DELETE: free plan fee limit exceeded"
                
        # Perform deletion and update statistics
        self.current_storage_size[file.storage] -= deleted_file_size
        del self.files[file_name]
        stats_to_update = self.monthly_stats[month_key][storage_name]
        stats_to_update.update_kb_sum += deleted_file_size 
        total_fees = self._calculate_total_fees(month_key)
        return f"DELETE: {total_fees['storage_fee']} {total_fees['update_fee']} {total_fees['usage_fee']}"

    def handle_update(self, timestamp: datetime, storage_name: str, file_name: str, new_size: int) -> str:
        """
        Handle file update operation.
        
        Validates the operation against storage availability and free plan limits.
        Updates storage statistics and calculates fees based on both original
        and new file sizes.
        
        Returns:
            Status message with operation result and fees if successful
        """
        month_key = self._get_month_key(timestamp)
        self._ensure_month_init(month_key)
        if storage_name not in STORAGE_UNITS:
            return f"UPDATE: invalid storage name"
        unit = STORAGE_UNITS[storage_name]
        if self.is_free_plan and not unit.is_free_plan_allowed:
            return "UPDATE: this storage location is not available on the free plan"
        if file_name not in self.files:
            return "UPDATE: file does not exist"
        file = self.files[file_name]
        if file.storage != storage_name:
            return "UPDATE: file is not in the specified storage"
        original_size_of_file_being_updated = file.size 
        
        # Check free plan limits
        if self.is_free_plan:
            current_stats_for_op_storage = self.monthly_stats[month_key][storage_name]
            # Calculate new storage size after update
            simulated_current_total_size_for_op_storage = self.current_storage_size.get(storage_name,0) + (new_size - original_size_of_file_being_updated)
            potential_max_size_kb = max(current_stats_for_op_storage.max_size, simulated_current_total_size_for_op_storage)
            # Update amount is sum of original and new sizes
            if self._would_exceed_free_plan_limit(month_key, storage_name, 
                                                 op_potential_max_size_kb=potential_max_size_kb, 
                                                 op_kb_for_current_op=original_size_of_file_being_updated + new_size): 
                return "UPDATE: free plan fee limit exceeded" 
                
        # Perform update and update statistics
        self.current_storage_size[storage_name] += (new_size - original_size_of_file_being_updated)
        stats_to_update = self.monthly_stats[month_key][storage_name]
        stats_to_update.max_size = max(stats_to_update.max_size, self.current_storage_size[storage_name])
        stats_to_update.update_kb_sum += (original_size_of_file_being_updated + new_size)
        file.size = new_size
        
        total_fees = self._calculate_total_fees(month_key)
        return f"UPDATE: {total_fees['storage_fee']} {total_fees['update_fee']} {total_fees['usage_fee']}"

    def handle_calc(self, timestamp: datetime) -> str:
        """
        Handle calculation operation for previous month's fees.
        
        Takes a snapshot of current storage sizes and marks update fees as settled.
        Returns storage sizes and fees for the previous month.
        
        Returns:
            Status message with storage sizes and fees
        """
        month_key = self._get_previous_month_key(timestamp)
        total_fees = self._calculate_total_fees(month_key)

        # Take snapshot of current storage sizes if not already taken
        if month_key not in self.calc_reported_sizes_snapshot:
            current_snapshot = {}
            for s_name in STORAGE_UNITS.keys():
                current_snapshot[s_name] = self.current_storage_size.get(s_name, 0)
                # Update end-of-month sizes for next month's initialization
                self.last_month_eom_storage_sizes[s_name] = self.current_storage_size.get(s_name, 0)
            self.calc_reported_sizes_snapshot[month_key] = current_snapshot

        # Report storage sizes and fees
        reported_sizes = self.calc_reported_sizes_snapshot[month_key]
        report_kb_a1 = reported_sizes.get('storage_A1', 0)
        report_kb_a2 = reported_sizes.get('storage_A2', 0)
        report_kb_b1 = reported_sizes.get('storage_B1', 0)
        report_kb_b2 = reported_sizes.get('storage_B2', 0)

        self.update_fee_settled_for_month.add(month_key)

        return (f"CALC: [{report_kb_a1} {report_kb_a2} {report_kb_b1} {report_kb_b2}] "
                f"{total_fees['storage_fee']} {total_fees['update_fee']} {total_fees['usage_fee']}")

def process_command(manager: StorageManager, command: str) -> str:
    """
    Process a single command string.
    
    Parses the command and routes it to the appropriate handler.
    Command format: <timestamp> <operation> [parameters...]
    
    Args:
        manager: StorageManager instance to handle the command
        command: Command string to process
        
    Returns:
        Result message from the operation handler
    """
    parts = command.strip().split()
    if not parts: return "ERROR: empty command"
    try:
        timestamp = datetime.fromisoformat(parts[0])
        cmd_type = parts[1].upper()
        if cmd_type == 'UPLOAD' and len(parts) == 5:
            return manager.handle_upload(timestamp, storage_name=parts[2], file_name=parts[3], size=int(parts[4]))
        elif cmd_type == 'DELETE' and len(parts) == 4:
            return manager.handle_delete(timestamp, storage_name=parts[2], file_name=parts[3])
        elif cmd_type == 'UPDATE' and len(parts) == 5:
            return manager.handle_update(timestamp, storage_name=parts[2], file_name=parts[3], new_size=int(parts[4]))
        elif cmd_type == 'CALC' and len(parts) == 2:
            return manager.handle_calc(timestamp)
        else:
            return f"{cmd_type}: invalid command format"
    except (ValueError, IndexError):
        return "ERROR: invalid command format or value"

def main():
    """
    Main entry point.
    
    Creates a StorageManager instance and processes commands
    from standard input.
    """
    manager = StorageManager(is_free_plan=True)
    for line in sys.stdin:
        if line.strip():
            print(process_command(manager, line))

if __name__ == "__main__":
    main()