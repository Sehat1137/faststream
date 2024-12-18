from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
)

from typing_extensions import override

from faststream.asyncapi.schema import (
    Channel,
    ChannelBinding,
    CorrelationId,
    Message,
    Operation,
)
from faststream.asyncapi.schema.bindings import kafka
from faststream.asyncapi.utils import resolve_payloads
from faststream.broker.types import MsgType
from faststream.exceptions import SetupError
from faststream.kafka.publisher.usecase import (
    BatchPublisher,
    DefaultPublisher,
    LogicPublisher,
)

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord

    from faststream.broker.types import BrokerMiddleware, PublisherMiddleware


class AsyncAPIPublisher(LogicPublisher[MsgType]):
    """A class representing a publisher."""

    def get_name(self) -> str:
        return f"{self.topic}:Publisher"

    def get_schema(self) -> Dict[str, Channel]:
        payloads = self.get_payloads()

        return {
            self.name: Channel(
                description=self.description,
                publish=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads, "Publisher"),
                        correlationId=CorrelationId(
                            location="$message.header#/correlation_id"
                        ),
                    ),
                ),
                bindings=ChannelBinding(kafka=kafka.ChannelBinding(topic=self.topic)),
            )
        }

    @overload  # type: ignore[override]
    @staticmethod
    def create(
        *,
        batch: Literal[True],
        key: Optional[bytes],
        topic: str,
        partition: Optional[int],
        headers: Optional[Dict[str, str]],
        reply_to: str,
        # Publisher args
        broker_middlewares: Sequence["BrokerMiddleware[Tuple[ConsumerRecord, ...]]"],
        middlewares: Sequence["PublisherMiddleware"],
        # AsyncAPI args
        schema_: Optional[Any],
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> "AsyncAPIBatchPublisher": ...

    @overload
    @staticmethod
    def create(
        *,
        batch: Literal[False],
        key: Optional[bytes],
        topic: str,
        partition: Optional[int],
        headers: Optional[Dict[str, str]],
        reply_to: str,
        # Publisher args
        broker_middlewares: Sequence["BrokerMiddleware[ConsumerRecord]"],
        middlewares: Sequence["PublisherMiddleware"],
        # AsyncAPI args
        schema_: Optional[Any],
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> "AsyncAPIDefaultPublisher": ...

    @overload
    @staticmethod
    def create(
        *,
        batch: bool,
        key: Optional[bytes],
        topic: str,
        partition: Optional[int],
        headers: Optional[Dict[str, str]],
        reply_to: str,
        # Publisher args
        broker_middlewares: Sequence[
            "BrokerMiddleware[Union[Tuple[ConsumerRecord, ...], ConsumerRecord]]"
        ],
        middlewares: Sequence["PublisherMiddleware"],
        # AsyncAPI args
        schema_: Optional[Any],
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> Union[
        "AsyncAPIBatchPublisher",
        "AsyncAPIDefaultPublisher",
    ]: ...

    @override
    @staticmethod
    def create(
        *,
        batch: bool,
        key: Optional[bytes],
        topic: str,
        partition: Optional[int],
        headers: Optional[Dict[str, str]],
        reply_to: str,
        # Publisher args
        broker_middlewares: Sequence[
            "BrokerMiddleware[Union[Tuple[ConsumerRecord, ...], ConsumerRecord]]"
        ],
        middlewares: Sequence["PublisherMiddleware"],
        # AsyncAPI args
        schema_: Optional[Any],
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> Union[
        "AsyncAPIBatchPublisher",
        "AsyncAPIDefaultPublisher",
    ]:
        if batch:
            if key:
                raise SetupError("You can't setup `key` with batch publisher")

            return AsyncAPIBatchPublisher(
                topic=topic,
                partition=partition,
                headers=headers,
                reply_to=reply_to,
                broker_middlewares=broker_middlewares,
                middlewares=middlewares,
                schema_=schema_,
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )
        else:
            return AsyncAPIDefaultPublisher(
                key=key,
                # basic args
                topic=topic,
                partition=partition,
                headers=headers,
                reply_to=reply_to,
                broker_middlewares=broker_middlewares,
                middlewares=middlewares,
                schema_=schema_,
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )


class AsyncAPIBatchPublisher(
    BatchPublisher,
    AsyncAPIPublisher[Tuple["ConsumerRecord", ...]],
):
    pass


class AsyncAPIDefaultPublisher(
    DefaultPublisher,
    AsyncAPIPublisher["ConsumerRecord"],
):
    pass
