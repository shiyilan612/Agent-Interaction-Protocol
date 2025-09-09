## 协议说明

### 简介
<p align="center"><img src="https://github.com/user-attachments/assets/b43e74cf-cb86-4d43-91c7-4409d84aa7c8" width = "600" height = "300"></p>
Agent Interaction Protocol（AIP）是一个面向多智能体协作的分布式智能体交互协议。AIP同时支持智能体之间的互联和智能体对工具的调用。除了点对点连接外，所有的智能体和工具都可作为一个节点挂载在同一个网关中以形成互联。

### 协议特点

- AIP同时支持中心代理和点对点通信。
- AIP采用gRPC作为底层通信协议，以二进制形式传输数据，拥有更快的序列化/反序列化速度。
- Agent间采用双向异步流的通信方式，支持全双工实时交互。

### 协议架构
<p align="center"><img src="https://github.com/user-attachments/assets/7e593a88-ab0a-42fc-bca1-fd04fe739755" width = "300" height = "240"></p>
AIP采用分层设计，模块层按照业务逻辑封装了一系列抽象类，对外抛出交互接口。会话层管理智能体间的对话，通过唯一会话标识与异步任务协同机制，确保消息的实时处理与资源安全。传输层负责会话层与gRPC接口之间的消息分发和消息接收。

-模块层：以节点或网关形式实现业务逻辑的分发与协调。
-会话层：管理节点间每次通信的生命流程。
-传输层：负责会话数据的接收与发送。

<p align="center"><img src="https://github.com/user-attachments/assets/e674e3c6-b13c-43d2-a8c5-efb306240bd7" width = "600" height = "400"></p>

AIP在通信中支持两种调用方式：请求-响应式与双向异步流式。在AIP中，智能体调用工具采用请求-响应式，智能体间通过双向异步流式进行全双工实时交互，由会话发起方负责发送会话关闭请求。

## PYTHON API

### Gateway

class Gateway
- 用途：提供一个中心代理服务，用于管理智能体代理 (Agent) 和工具 (Tool) 的注册和与通信。Gateway 作为一个核心组件，负责维护网络中所有节点的信息，并保障节点间的消息路由和通信。
- 属性说明： 
  - address (str)：网关服务的部署地址，如 "localhost:50051"
  - gateway_id (str)：网关的唯一标识符，如果未提供则自动生成
- 方法说明： 
  - start()：初始化并启动网关服务，使其能够接收智能体代理和工具的注册请求并处理路由
  - stop()：停止网关服务器，释放资源
  - get_registered_nodes()：获取所有已注册的节点（Agent和Tool）的信息 
    - 返回：{节点ID：节点信息}的字典 (Dict[str, Union[AgentInfo, ToolInfo]])
  - get_registered_agents()：获取所有已注册的智能体代理 Agent 信息
    - 返回：{代理ID：代理信息}的字典 (Dict[str, AgentInfo])
  - get_registered_tools()：获取所有已注册的工具 Tool 信息
    - 返回：{工具ID：工具信息}的字典 (Dict[str, ToolInfo])


### Agent
class Agent
- 用途：提供创建、管理和运行Agent的功能接口。Agent 可以接收、发送消息与其它 Agent 进行双向流式通信，可以发送同步请求调用 Tool 执行特定任务。

- 属性说明
  - address (str)：智能体代理服务的部署地址
  - agent_id (str)：智能体代理的唯一标识符
  - name (str)：智能体代理的名称
  - domain (str)：智能体代理所属的领域或分组
  - description (str)：智能体代理功能的详细描述
  - version (str)：智能体代理版本
  - input_mode (List[Mode])：智能体代理可接受的输入模态列表，包括文本模态TEXT 、图像模态IMAGE、音频模态AUDIO、嵌入模态EMBEDDED
  - output_mode (List[Mode])：智能体代理可提供的输出模态列表，包括文本模态TEXT 、图像模态IMAGE、音频模态AUDIO、嵌入模态EMBEDDED
  - skills (List[AgentSkill])：智能体代理具备的技能列表
  - agent_info (AgentInfo)：包含智能体代理信息的对象，用于向网关注册

- 方法说明

  - _create_agent_info() -> AgentInfo：创建一个 AgentInfo 对象用于向网关注册
    - 返回：一个 AgentInfo 对象

  - start() -> Agent：启动智能体代理服务器
    - 返回：self

  - stop()：停止智能体代理服务器并关闭所有客户端连接

  - register_to_gateway(gateway_address) -> Agent：向网关注册智能体代理
    - 参数：gateway_address (str)：网关服务的地址
    - 返回：self

  - update_peers(domain="default") -> Agent：查询网关中所有的节点（Agent和Tool）清单，并更新当前 Agent 服务中存储的 peers 节点信息
    - 参数：domain (str)：要查询的领域。可选参数，默认为 "default"
    - 返回：self

  - get_nodes() -> Dict[str, Union[AgentInfo, Any]]：查询当前 Agent 服务中存储的 peers 节点信息
    - 返回：节点信息字典

  - create_task_info(parent_task_ids=None) -> TaskInfo：创建一个新的任务信息对象
    - 参数：parent_task_ids (List[str])：父任务 ID 列表
    - 返回：新的TaskInfo 对象

  - create_content_items(content, content_mode=None) -> List[ContentItem]：创建消息中的 ContentItem 对象列表
    - 参数： 
      content (Union[str, bytes, List[Union[str, bytes]]])：消息内容，可以是列表或单个内容，内容的类型支持string和bytes两类。其中，文本模态使用string类型，图像、音频、嵌入模态使用bytes类型。
      content_mode (Optional[Union[Mode, List[Mode]]])：消息内容的模态或模态列表，与消息列表元素一一对应。
    - 返回：ContentItem 对象列表

  - create_agent_message(receiver_id, content, session_id=None, task_info=None, content_mode=None, session_status=SessionStatus.START_QUEST, message_id=None, reply_to_message_id=None) -> AgentMessage：创建一个 AgentMessage 对象，用于智能体代理之间的交互

    - 参数： 
      receiver_id (str)：接收消息的智能体代理 ID
      content (Union[str, bytes, List[Union[str, bytes]]])：消息的内容
      session_id (str,)：会话 ID
      task_info (TaskInfo, 可选)：任务信息
      content_mode (Optional[Union[Mode, List[Mode]]], 可选)：消息内容的模态，与消息列表元素一一对应。
      session_status (SessionStatus, 可选)：会话状态
      message_id (str, 可选)：当前消息的ID
      reply_to_message_id (str, 可选)：用于标记当前消息是对某消息 ID的回复
    - 返回：AgentMessage 对象

  - create_agent_client(receiver_id, stub=GatewayServiceStub, stream_calling="RouteAgentCalling") -> str：创建一个 AgentClient 对象，用于向其他智能体代理发送消息
    - 参数： 
      receiver_id (str)：接收消息的智能体代理 ID
      stub(GatewayServiceStub)：网关存根类
      stream_calling(str)：Agent 消息路由方法
    - 返回：创建的 AgentClient 对象的 session_id
  - submit_inquiry(session_id, receiver_id, content, content_mode=None, session_status=None,  task_info=None)：通过网关向另一个智能体代理发送请求
    - 参数： 
      session_id (str)：会话 ID
      receiver_id (str)：接收请求的智能体代理 ID
      content (Union[str, bytes, List[Union[str, bytes]]])：消息的内容
      content_mode (Optional[Union[Mode, List[Mode]]], 可选)：消息内容的模态，与消息列表元素一一对应。
      session_status (SessionStatus, 可选)：会话状态
      task_info (TaskInfo, 可选)：任务信息

  - receive_feedback(session_id, receiver_id) -> AgentMessage：从另一个智能体代理服务端接收响应
    - 参数： 
      session_id (str)：会话 ID
      receiver_id (str)：服务端智能体代理 ID
    - 返回：AgentMessage 对象

  - submit_feedback(session_id, receiver_id, content, content_mode=None, session_status=None,  task_info=None)：向另一个智能体代理客户端发送响应
    - 参数： 
      session_id (str)：会话 ID
      receiver_id (str)：接收响应的智能体代理 ID
      content (Union[str, bytes, List[Union[str, bytes]]])：消息的内容
      content_mode (Optional[Union[Mode, List[Mode]]], 可选)：消息内容的模态，与消息列表元素一一对应。
      session_status (SessionStatus, 可选)：会话状态
      task_info (TaskInfo, 可选)：任务信息

  - receive_inquiry() -> AgentMessage：从另一个智能体代理客户端接收请求
    - 返回：AgentMessage 对象

  - invoke_session_handlers(session_id, handlers) -> List[Any]：从另一个智能体代理服务端接收响应
    - 参数： 
      session_id (str)：会话 ID
      handlers (List[Callable])：自定义请求处理函数列表，每个handler接收 AgentMessage 、处理请求并返回处理后的响应信息
    - 返回：响应信息的列表

  - create_tool_request(receiver_id, session_id=None, tool_name=None, arguments=None) -> ToolRequest：创建一个 ToolRequest 对象，用于发送工具调用请求
    - 参数： 
      receiver_id (str)：要调用的工具 ID
      session_id (str, 可选)：会话 ID
      tool_name (str, 可选)：要调用的工具名称
      arguments (Dict[str, Any], 可选)：工具调用的参数
    - 返回：ToolRequest 对象

  - call_tool(tool_id, session_id=None, tool_name=None, arguments=None) -> ToolResponse：通过网关调用具有给定 ID 的工具
    - 参数： 
      tool_id (str)：要调用的工具 ID
      session_id (str, 可选)：会话 ID
      tool_name (str, 可选)：工具名称
      arguments (Dict[str, Any], 可选)：工具调用参数
    - 返回：工具的响应ToolResponse对象

  - close_agent_client(session_id, receiver_id) -> bool：关闭特定的代理客户端
    - 参数：
      session_id(str)：会话 ID
      receiver_id(str)：接收响应的智能体代理 ID
    - 返回：如果客户端被关闭则为 True，否则为 False

  - close_tool_client(tool_id) -> bool：关闭特定的工具客户端
    - 参数：tool_id (str)：工具的 ID
    - 返回：如果客户端被关闭则为 True，否则为 False


### Tool
class Tool
- 用途：提供创建、管理和执行工具的功能接口。Tool 由 Agent 发起调用请求，执行特定任务，并返回响应结果。
- 属性说明：
  - address (str)：工具服务的部署地址
  - tool_id (str)：工具的唯一标识符
  - name (str)：工具的名称
  - domain (str)：工具所属的领域或分组
  - description (str)：工具功能的详细描述
  - version (str)：工具版本
  - input_mode (List[Mode])：工具可接受的输入模态列表，包括文本模态TEXT 、图像模态IMAGE、音频模态AUDIO、嵌入模态EMBEDDED
  - output_mode (List[Mode])：工具可提供的输出模态列表，包括文本模态TEXT 、图像模态IMAGE、音频模态AUDIO、嵌入模态EMBEDDED
  - arguments (Dict[str, str])：工具的参数名称及其赋值的字典
  - tool_info (ToolInfo)：包含工具信息的对象，用于向网关注册

- 方法说明
  - update_tool_info：向网关更新最新的ToolInfo
    - 参数： 
      name (str)：工具的名称
      description (str)：工具功能的详细描述
      domain (str)：工具所属的领域或分组
      version (str)：工具版本
      input_mode (List[Mode])：工具可接受的输入模态列表，包括文本模态TEXT 、图像模态IMAGE、音频模态AUDIO、嵌入模态EMBEDDED
      output_mode (List[Mode])：工具可提供的输出模态列表，包括文本模态TEXT 、图像模态IMAGE、音频模态AUDIO、嵌入模态EMBEDDED
      arguments (Dict[str, str])：工具的参数名称及其赋值的字典
    - 返回：self

  - set_process_request_handler(handler)：设置处理传入工具请求的函数
    - 参数：handler (Callable)：自定义请求处理函数，接收 ToolRequest 、执行工具并返回 ToolResponse
    - 返回：self

  - start()：启动工具服务器
    - 返回：self

  - stop()：停止工具服务器

  - register_to_gateway(gateway_address)：向网关注册工具
    - 参数：gateway_address (str)：网关服务的地址
    - 返回：self

  - creat_function_tool(function, address, ...)：@classmethod 类方法，用于创建基于外部自定义的Python函数工具的服务
    - 参数： 
      function (Callable)：要封装为Tool的外部Python函数工具
      address (str)：工具服务的地址
      其他参数与 Tool 类初始化参数相同
    - 返回：初始化的 Tool 实例

  - creat_api_tool(address, api_url, api_method, api_headers, api_timeout, ...)：@classmethod 类方法，用于创建基于外部自定义的 API 工具的服务
    - 参数： 
      address (str)：工具服务的地址
      api_url (str)：API 的 URL 路径
      api_method(str)：API 请求的 HTTP 方法，如 GET、POST、PUT、DELETE
      api_headers(Dict[str, str])：API 请求的头部信息，包含请求头中的各种信息，如认证令牌等
      api_timeout(int)：API 请求的超时时间，以秒为单位
      其他参数与 Tool 类初始化参数相同
    - 返回：初始化的 Tool 实例

### Memory
class ContextMemory
- 用途：提供Agent历史消息的存储，访问，管理。目前基于TinyDB，数据存储方式为Json。
- 属性说明
  - agent_id (str)：智能体代理的唯一标识符
  - save_dir (str)：存储路径
  - dbs (Dict)：TinyDB数据库字典
- 方法说明
  - init(self, agent_id, save_root="./.cache"):：初始化一个ContextMemory实例
  - create(task_id) -> bool：检查指定路径的数据库文件是否已存在，不存在则创建一个新的 TinyDB 数据库实例。
    - 参数：
      task_id(str)：任务的唯一标识（如 "task_001"），用于创建对应的任务数据库
    - 返回：成功创建数据库则为 True，数据库已存在或创建失败则为 False
  - write(task_id, message) -> bool：将消息转换为字典格式并插入到对应任务的数据库中
    - 参数：
      task_id(str)：任务的唯一标识（如 "task_001"），用于定位对应的数据库
      message(Union[AgentMessage, ToolRequest, ToolResponse])：需要写入的消息
    - 返回：成功写入消息 True，任务数据库不存在或写入失败则为 False
  - clear(task_id) -> bool：清空对应任务的数据库中的所有记录
    - 参数：task_id(str)：任务的唯一标识（如 "task_001"），用于定位对应的数据库
    - 返回：成功清空数据库则为 True，任务数据库不存在或清空失败则为 False

### Protocol Data

Mode (Enum)
- 用途：定义传输或处理的数据的类型。
- 属性说明：  
  - TEXT (0)：文本类型数据。  
  - IMAGE (1)：图片类型数据（二进制格式）。  
  - AUDIO (2)：音频类型数据（二进制格式）。  
  - EMBEDDED (3)：自定义二进制数据（如序列化对象）。
- 示例：  
mode = Mode.TEXT  # 指定当前内容为文本类型

---
SessionStatus (Enum)
- 用途：表示会话的状态。
- 属性说明：  
  - START_QUEST (0)：会话开始，发送第一个请求（客户端）。  
  - HOLD_QUEST (1)：在会话开始后，发起请求（客户端）。  
  - HOLD_RESPONSE (2)：在会话开始后，回复的响应（服务端）。  
  - STOP_QUEST (3)：在会话开始后，发起任务终止的请求（客户端）。  
  - STOP_RESPONSE (4)：遇到超时或错误，会话异常终止；或者接到客户端主动发送的终止请求（服务端）。
- 示例：  
status = SessionStatus.START_QUEST  # 标记会话为“任务开始”状态

---
TaskStatus (Enum)
- 用途：表示任务的执行状态。
- 属性说明：  
  - CREATE (0)：任务已创建但未执行。  
  - EXECUTING (1)：任务正在执行中。  
  - WAITING (2)：任务等待依赖项完成。  
  - BREAK (3)：任务被中断。  
  - FINISH (4)：任务已完成。
- 示例：  
task_status = TaskStatus.EXECUTING  # 标记任务为“执行中”

---
AgentSkill
- 用途：描述Agent的技能信息。
- 属性说明：  
  - skill_id (str)：技能的唯一标识（如 "translation"）。  
  - capability (str)：技能的功能描述（如 "多语言翻译"）。
- 示例：  
skill = AgentSkill(skill_id="translation", capability="多语言翻译")

---
TaskInfo
- 用途：存储任务的基本信息。
- 属性说明：  
  - task_id (str)：任务的唯一标识（如 "task_001"）。  
  - parent_task_ids (List[str])：父任务ID列表（用于任务依赖，如 ["task_000"]）。  
  - task_status (TaskStatus)：任务状态（基于 TaskStatus 枚举）。
- 示例：  
task = TaskInfo(task_id="task_001", task_status=TaskStatus.CREATE)

---
ContentItem
- 用途：封装消息内容，支持多种数据类型。
- 属性说明：  
  - _text (Optional[str])：文本内容（如 "Hello"）。  
  - _image (Optional[bytes])：图片二进制数据（如 b"\x89PNG..."）。  
  - _audio (Optional[bytes])：音频二进制数据（如 b"WAV..."）。  
  - _embedded (Optional[bytes])：自定义二进制数据（如序列化对象）。
- 方法说明：  
  - write_text(str)：创建文本内容项。  
  - write_image(bytes)：创建图片内容项。  
  - write_audio(bytes)：创建音频内容项。  
  - write_embedded(bytes)：创建自定义二进制内容项。
- 示例：  
text_item = ContentItem.write_text("Hello, World!")
image_item = ContentItem.write_image(b"image_data")

---
Peer
- 用途：表示通信的对等节点（Agent或工具）。
- 属性说明：  
  - _agent_info (Optional[AgentInfo])：Agent的详细信息（如ID、地址等）。  
  - _tool_info (Optional[ToolInfo])：工具的详细信息（如参数、模式等）。
- 示例：  
peer = Peer()
peer.agent_info = AgentInfo(agent_id="agent_001")

---
AgentInfo
- 用途：描述Agent的详细信息。
- 属性说明：  
  - agent_id (str)：Agent的唯一标识（如 "agent_001"）。  
  - address (str)：Agent的网络地址（如 "http://localhost:8080"）。  
  - name (str)：Agent的名称（如 "翻译智能体"）。  
  - domain (str)：Agent所属的领域（如 "translation"）。  
  - input_mode (List[Mode])：Agent支持的输入模式列表（如 [Mode.TEXT]）。  
  - output_mode (List[Mode])：Agent支持的输出模式列表（如 [Mode.TEXT]）。  
  - description (str)：Agent的功能描述（如 "支持中英文翻译"）。  
  - skills (List[AgentSkill])：Agent的技能列表（如 [AgentSkill(...)]）。  
  - version (str)：Agent的版本号（如 "v1.0.0"）。
- 示例：  
agent = AgentInfo(agent_id="agent_001", input_mode=[Mode.TEXT])

---
RegisterAgentResponse
- 用途：Agent注册后的响应结果。
- 属性说明：  
  - success (bool)：注册是否成功（True/False）。  
  - peers (List[Peer])：已知的节点列表（如 [Peer(...), Peer(...)]）。
- 示例：  
response = RegisterAgentResponse(success=True, peers=[peer1, peer2])

---
ToolInfo
- 用途：描述工具的详细信息。
- 属性说明：  
  - tool_id (str)：工具的唯一标识（如 "tool_001"）。  
  - address (str)：工具的网络地址（如 "http://localhost:8081"）。  
  - name (str)：工具的名称（如 "OCR工具"）。  
  - domain (str)：工具所属的领域（如 "image_processing"）。  
  - input_mode (List[Mode])：工具支持的输入模式列表（如 [Mode.IMAGE]）。  
  - output_mode (List[Mode])：工具支持的输出模式列表（如 [Mode.TEXT]）。  
  - description (str)：工具的功能描述（如 "图片转文字"）。  
  - arguments (Dict[str, str])：工具调用所需的参数（如 {"lang": "en"}）。  
  - version (str)：工具的版本号（如 "v2.1.0"）。
- 示例：  
tool = ToolInfo(tool_id="tool_001", arguments={"lang": "en"})

---
RegisterToolResponse
- 用途：工具注册后的响应结果。
- 属性说明：  
  - success (bool)：注册是否成功（True/False）。
- 示例：  
response = RegisterToolResponse(success=True)

---
DeregisterNodeRequest
- 用途：请求网关注销节点。
- 属性说明：  
  - node_id (str)：发送请求的节点（Agent或工具）的ID（如 "agent_001"/"tool_001"）。
- 示例：  
request = DeregisterNodeRequest(node_id="agent_001")

---
DeregisterNodeResponse
- 用途：节点注销后的响应结果。
- 属性说明：  
  - success (bool)：注销是否成功（True/False）。
- 示例：  
response = DeregisterNodeResponse(success=True)

---
GetNodesRequest
- 用途：请求获取指定域内的节点信息。
- 属性说明：  
  - agent_id (str)：发送请求的Agent ID（如 "agent_001"）。  
  - domain (str)：目标域名称（如 "translation"）。
- 示例：  
request = GetNodesRequest(agent_id="agent_001", domain="translation")

---
GetNodesResponse
- 用途：返回指定域内的节点列表。
- 属性说明：  
  - peers (List[Peer])：节点列表（如 [Peer(...), Peer(...)]）。
- 示例：  
response = GetNodesResponse(peers=[peer1, peer2])

---
AgentMessage
- 用途：Agent间通信的消息体。
- 属性说明：  
  - sender_id (str)：发送方ID（如 "agent_001"）。  
  - receiver_id (str)：接收方ID（如 "agent_002"）。  
  - session_id (str)：会话的唯一标识（如 "session_123"）。  
  - session_status (SessionStatus)：会话状态（基于 SessionStatus 枚举）。  
  - task_info (TaskInfo)：关联的任务信息（如 TaskInfo(...)）。  
  - message_id (str)：消息的唯一标识（如 "msg_001"）。  
  - reply_to_message_id (str)：回复的目标消息ID（如 "msg_000"）。  
  - content (List[ContentItem])：消息内容列表（如 [ContentItem(...)]）。
- 方法说明：  
  - add_content(item: ContentItem)：向消息中添加内容项。
- 示例：  
msg = AgentMessage(sender_id="agent_001", receiver_id="agent_002")
msg.add_content(ContentItem.write_text("Hello!"))

---
ToolRequest
- 用途：向工具发送的请求。
- 属性说明：  
  - sender_id (str)：发送方ID（如 "agent_001"）。  
  - receiver_id (str)：接收方ID（如 "tool_001"）。  
  - session_id (str)：会话的唯一标识（如 "session_123"）。  
  - tool_name (str)：目标工具名称（如 "translator"）。  
  - arguments (Dict[str, str])：调用参数（如 {"text": "Hello"}）。
- 示例：  
request = ToolRequest(tool_name="translator", arguments={"text": "Hello"})

---
ToolResponse
- 用途：工具执行后的响应。
- 属性说明：  
  - sender_id (str)：发送方ID（如 "tool_001"）。  
  - receiver_id (str)：接收方ID（如 "agent_001"）。  
  - session_id (str)：会话的唯一标识（如 "session_123"）。  
  - is_error (bool)：是否发生错误（True/False）。  
  - error_message (str)：错误描述（如 "Invalid input"）。  
  - content (List[ContentItem])：响应内容列表（如 [ContentItem(...)]）。
- 方法说明：  
  - add_content(item: ContentItem)：向响应中添加内容项。
- 示例：  
response = ToolResponse(is_error=False, content=[text_item])

---
HeartbeatRequest
  - 用途：向网关定时发送的心跳检测请求。
  - 属性说明：  
    - sender_id (str)：发送请求的节点（Agent或工具）的ID（如 "agent_001"/"tool_001"）。  
  - 示例：  
request = HeartbeatRequest(sender_id="agent_001")

---
HeartbeatResponse
  - 用途：向工具发送的请求。
  - 属性说明：  
    - success(bool)：心跳检测请求是否成功响应（True/False）。  
    - message(str)：响应信息。  
  - 示例：  
response = HeartbeatResponse(success=True, message="Heartbeat received")



