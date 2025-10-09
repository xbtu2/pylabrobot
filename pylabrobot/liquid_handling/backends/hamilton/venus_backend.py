""" Venus backend for PyLabRobot """

from __future__ import annotations

import logging
from typing import List, Union, Optional, cast

from pylabrobot.liquid_handling.backends.backend import LiquidHandlerBackend
from pylabrobot.liquid_handling.standard import (
  Pickup,
  Drop,
  SingleChannelAspiration,
  SingleChannelDispense,
  PickupTipRack,
  DropTipRack,
  MultiHeadAspirationPlate,
  MultiHeadAspirationContainer,
  MultiHeadDispensePlate,
  MultiHeadDispenseContainer,
  ResourcePickup,
  ResourceDrop,
  ResourceMove,
)
from pylabrobot.resources import Resource, TipSpot, Well, Plate, TipRack, Trash, Container, Tube
from pylabrobot.resources.errors import HasTipError, NoTipError, TooLittleLiquidError

# pyhamilton is a fictional library used for this example.
# If it were real, you would install it e.g. `pip install pyhamilton`

from .pyhamilton import HamiltonInterface, LayoutManager
from .pyhamilton import (
  INITIALIZE,
  PICKUP, EJECT, ASPIRATE, DISPENSE,
  PICKUP96, EJECT96, ASPIRATE96, DISPENSE96,
  ISWAP_GET, ISWAP_PLACE, ISWAP_MOVE
)
from .pyhamilton.oemerr import PositionError, HamiltonError


logger = logging.getLogger("pylabrobot")

# Default liquid class from pace_util.py, used when no other is provided.
DEFAULT_LIQUID_CLASS = "StandardVolumeFilter_Water_DispenseSurface_Part"


class VenusBackend(LiquidHandlerBackend):
  """
  Backend for Hamilton robots using the Venus software, through the pyhamilton library.

  This backend communicates with the robot via the `pyhamilton` library, which is assumed to be
  installed and configured. It uses a higher-level command interface than the STAR backend.
  """

  def __init__(
    self,
    **kwargs
  ):
    """ Create a new Venus backend. """
    super().__init__(**kwargs)
    self.hamilton_interface: Optional[HamiltonInterface] = None
    self._num_channels = 16 # Defaulting to 16 channels, can be updated in setup.

  def can_pick_up_tip(self, channel_idx, tip):
    return True

  @property
  def num_channels(self) -> int:
    """The number of pipette channels present on the robot."""
    return self._num_channels

  async def setup(self, raise_first_exception=True, **kwargs):
    """
    Set up the Venus backend.

    This method establishes a connection with the Hamilton robot using `pyhamilton`,
    initializes the robot, and loads the specified deck layout.

    Args:
      layfile_path: The path to the Hamilton `.lay` file describing the deck layout.
    """
    logger.info("Setting up Venus backend.")

    # In a real implementation, connection details would be passed to HamiltonInterface.
    self.hamilton_interface = HamiltonInterface(windowed=True)
    self.hamilton_interface.start()
    logger.info("Connected to Hamilton robot.")
    # self.layout_manager = LayoutManager(layfile_path)

    logger.info("Initializing robot.")
    try:
      init_cmd = self.hamilton_interface.send_command(INITIALIZE)
      self.hamilton_interface.wait_on_response(init_cmd, raise_first_exception=raise_first_exception)
      logger.info("Robot initialization complete.")
    except HamiltonError as e:
      logger.error("Failed to initialize robot: %s", e)
      raise

    await super().setup(**kwargs)

  async def stop(self):
    """ Stop the Venus backend. """
    if self.hamilton_interface:
      self.hamilton_interface.stop()
      self.hamilton_interface = None
    await super().stop()

  def _resource_to_pos_str(self, resource: Resource, position_in_parent: int = 0) -> str:
    """Converts a PyLabRobot resource to a pyhamilton position string."""
    # For plates/tipracks being moved, the resource itself is what we care about.
    if isinstance(resource, (Plate, TipRack)):
        labware_name = resource.name
        # iSWAP commands often target the resource as a whole, using the first well/position as ref
        return f"{labware_name}, A1"
    else: # For wells, tip spots, etc.
        labware_name = resource.parent.name
        pos_id_str= resource.name
        # pyhamilton uses 1-based indexing for positions
        pos_id_col = int(pos_id_str.split("_")[-2])
        pos_id_row = int(pos_id_str.split("_")[-1])
        if isinstance(resource, TipSpot):
          pos_id = pos_id_col * resource.parent.num_items_y + pos_id_row + 1
        elif isinstance(resource, Well):
          pos_id = chr(ord('A') + pos_id_row) + str(pos_id_col + 1)
        elif isinstance(resource, Tube):
          pos_id = labware_name.split("-")[-1]  # Assuming tube car end with their position number
          labware_name = resource.parent.parent.name  # Get the tube rack name
    return f"{labware_name}, {pos_id}"

  def _ops_to_channel_info(
    self,
    ops: Union[List[Pickup], List[Drop], List[SingleChannelAspiration], List[SingleChannelDispense]],
    use_channels: List[int]
  ) -> tuple[str, str]:
    """
    Convert a list of operations to `labwarePositions` and `channelVariable` strings
    for `pyhamilton`.
    """
    channels_mask = ["0"] * self.num_channels
    for i in use_channels:
      channels_mask[i] = "1"
    channel_variable = "".join(channels_mask)

    positions = [self._resource_to_pos_str(op.resource) for op in ops]
    labware_positions_str = ";".join(positions)

    return labware_positions_str, channel_variable

  async def pick_up_tips(self, ops: List[Pickup], use_channels: List[int], **kwargs):
    """ Pick up tips from the deck. """
    labware_positions, channel_variable = self._ops_to_channel_info(ops, use_channels)
    params = {"labwarePositions": labware_positions, "channelVariable": channel_variable, **kwargs}
    logger.debug("Picking up tips with params: %s", params)
    try:
      cmd = self.hamilton_interface.send_command(PICKUP, **params)
      self.hamilton_interface.wait_on_response(cmd, raise_first_exception=True)
    except HamiltonError as e:
      if "No tip" in str(e):
        raise NoTipError("Failed to pick up tip, none present.") from e
      raise

  async def drop_tips(self, ops: List[Drop], use_channels: List[int], **kwargs):
    """ Drop tips on the deck. """
    is_trash = all(isinstance(op.resource, Trash) for op in ops)
    if is_trash:
      params = {"useDefaultWaste": 1, "channelVariable": self._ops_to_channel_info(ops, use_channels)[1], **kwargs}
    else:
      labware_positions, channel_variable = self._ops_to_channel_info(ops, use_channels)
      params = {"labwarePositions": labware_positions, "channelVariable": channel_variable, **kwargs}
    logger.debug("Dropping tips with params: %s", params)
    try:
      cmd = self.hamilton_interface.send_command(EJECT, **params)
      self.hamilton_interface.wait_on_response(cmd, raise_first_exception=True)
    except HamiltonError as e:
      if "Tip already fitted" in str(e):
        raise HasTipError("Attempted to drop tip when another was already fitted.") from e
      raise

  async def aspirate(self, ops: List[SingleChannelAspiration], use_channels: List[int], **kwargs):
    """ Aspirate liquid. """
    labware_positions, channel_variable = self._ops_to_channel_info(ops, use_channels)
    volumes = [op.volume for op in ops]
    params = {
      "labwarePositions": labware_positions, "volumes": volumes, "channelVariable": channel_variable,
      "liquidClass": kwargs.get("liquidClass", DEFAULT_LIQUID_CLASS),
      # **kwargs
    }
    logger.debug("Aspirating with params: %s", params)
    try:
      cmd = self.hamilton_interface.send_command(ASPIRATE, **params)
      self.hamilton_interface.wait_on_response(cmd, raise_first_exception=True)
    except HamiltonError as e:
      if "not enough liquid" in str(e).lower():
        raise TooLittleLiquidError from e
      raise

  async def dispense(self, ops: List[SingleChannelDispense], use_channels: List[int], **kwargs):
    """ Dispense liquid. """
    labware_positions, channel_variable = self._ops_to_channel_info(ops, use_channels)
    volumes = [op.volume for op in ops]
    params = {
      "labwarePositions": labware_positions, "volumes": volumes, "channelVariable": channel_variable,
      "liquidClass": kwargs.get("liquidClass", DEFAULT_LIQUID_CLASS),
      "dispenseMode": kwargs.get("dispenseMode", 8),
      # **kwargs
    }
    logger.debug("Dispensing with params: %s", params)
    cmd = self.hamilton_interface.send_command(DISPENSE, **params)
    self.hamilton_interface.wait_on_response(cmd, raise_first_exception=True)

  def _resource_to_96_pos_str(self, resource: Union[Plate, TipRack, Container]) -> str:
    """ Generate a semicolon-separated position string for all 96 positions. """
    if isinstance(resource, Container):
        # For a single container, we might just use its name for all 96 channels.
        # This behavior depends on the specifics of the pyhamilton implementation.
        # A common approach is to provide the same position string for all channels.
        pos_str = f"{resource.name}, 1" # Assuming position '1' for a generic container
        return ";".join([pos_str] * 96)

    positions = [self._resource_to_pos_str(item) for item in resource.children]
    return ";".join(positions)

  async def pick_up_tips96(self, pickup: PickupTipRack, **kwargs):
    """ Pick up a full rack of 96 tips. """
    labware_positions = self._resource_to_96_pos_str(pickup.resource)
    params = {"labwarePositions": labware_positions, **kwargs}
    logger.debug("Picking up 96 tips with params: %s", params)
    cmd = self.hamilton_interface.send_command(PICKUP96, **params)
    self.hamilton_interface.wait_on_response(cmd, raise_first_exception=True)

  async def drop_tips96(self, drop: DropTipRack, **kwargs):
    """ Drop a full rack of 96 tips. """
    params = {}
    if isinstance(drop.resource, Trash):
      params["tipEjectToKnownPosition"] = 2 # 2 is default waste
    else:
      params["labwarePositions"] = self._resource_to_96_pos_str(drop.resource)
    params.update(kwargs)
    logger.debug("Dropping 96 tips with params: %s", params)
    cmd = self.hamilton_interface.send_command(EJECT96, **params)
    self.hamilton_interface.wait_on_response(cmd, raise_first_exception=True)

  async def aspirate96(self, aspiration: Union[MultiHeadAspirationPlate, MultiHeadAspirationContainer], **kwargs):
    """ Aspirate from 96 wells or a container simultaneously. """
    if isinstance(aspiration, MultiHeadAspirationPlate):
        resource = aspiration.wells[0].parent
    else: # MultiHeadAspirationContainer
        resource = aspiration.container
    labware_positions = self._resource_to_96_pos_str(resource)
    params = {
      "labwarePositions": labware_positions,
      "aspirateVolume": aspiration.volume,
      "liquidClass": kwargs.get("liquidClass", DEFAULT_LIQUID_CLASS),
      # **kwargs
    }
    logger.debug("Aspirating 96 with params: %s", params)
    cmd = self.hamilton_interface.send_command(ASPIRATE96, **params)
    self.hamilton_interface.wait_on_response(cmd, raise_first_exception=True)

  async def dispense96(self, dispense: Union[MultiHeadDispensePlate, MultiHeadDispenseContainer], **kwargs):
    """ Dispense to 96 wells or a container simultaneously. """
    if isinstance(dispense, MultiHeadDispensePlate):
        resource = dispense.wells[0].parent
    else: # MultiHeadDispenseContainer
        resource = dispense.container
    labware_positions = self._resource_to_96_pos_str(resource)
    params = {
      "labwarePositions": labware_positions,
      "dispenseVolume": dispense.volume,
      "liquidClass": kwargs.get("liquidClass", DEFAULT_LIQUID_CLASS),
      # **kwargs
    }
    logger.debug("Dispensing 96 with params: %s", params)
    cmd = self.hamilton_interface.send_command(DISPENSE96, **params)
    self.hamilton_interface.wait_on_response(cmd, raise_first_exception=True)

  async def pick_up_resource(self, pickup: ResourcePickup, **kwargs):
    """ Pick up a resource (e.g., a plate) using the iSWAP. """
    source_pos = self._resource_to_pos_str(pickup.resource)
    params = {"plateLabwarePositions": source_pos, **kwargs}
    logger.debug("iSWAP getting resource with params: %s", params)
    try:
      cmd = self.hamilton_interface.send_command(ISWAP_GET, **params)
      self.hamilton_interface.wait_on_response(cmd, raise_first_exception=True, timeout=120)
    except (PositionError, HamiltonError) as e:
      logger.error("iSWAP error during pick_up_resource: %s", e)
      raise

  async def drop_resource(self, drop: ResourceDrop, **kwargs):
    """ Drop a resource (e.g., a plate) using the iSWAP. """
    dest_pos = self._resource_to_pos_str(drop.resource)
    params = {"plateLabwarePositions": dest_pos, **kwargs}
    logger.debug("iSWAP placing resource with params: %s", params)
    try:
      cmd = self.hamilton_interface.send_command(ISWAP_PLACE, **params)
      self.hamilton_interface.wait_on_response(cmd, raise_first_exception=True, timeout=120)
    except (PositionError, HamiltonError) as e:
      logger.error("iSWAP error during drop_resource: %s", e)
      raise

  async def move_picked_up_resource(self, move: ResourceMove, **kwargs):
    """ Move a picked up resource to a new location. """
    raise NotImplementedError("move_picked_up_resource is not implemented for this backend. "
                              "This operation requires translating absolute coordinates to "
                              "deck layout positions, which is not supported.")
