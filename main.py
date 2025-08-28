import time
from datetime import datetime
from typing import Dict, Any, Optional
import json
import traceback

import functools

from dotenv import load_dotenv
import os

import uuid

import httpx

import requests

import re

import base64

import logging
import tiktoken

import json

from fastapi import FastAPI, APIRouter
from fastapi.responses import JSONResponse
from fastapi import Request
from fastapi import HTTPException

from langgraph.graph.message import add_messages
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage, ToolMessage, BaseMessage
from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import tools_condition
from langgraph.prebuilt import ToolNode
from langgraph.errors import NodeInterrupt
from langgraph.checkpoint.memory import MemorySaver
from langgraph.checkpoint.memory import InMemorySaver
from langchain_openai import ChatOpenAI

import openai

import unicodedata

from contextlib import asynccontextmanager
from pydantic import BaseModel, Field
from typing import TypedDict, Dict, List, Optional, Annotated, Literal, Union

import asyncio
import aiohttp  # Para requisições HTTP assíncronas

from functools import partial

from functools import wraps

from fastapi.responses import ORJSONResponse

from langgraph.graph import StateGraph


import pytz
from datetime import timezone, timedelta, datetime
from dateutil import parser
import time
from datetime import datetime, timedelta
import pytz

import re

from apscheduler.schedulers.background import BackgroundScheduler

import pandas as pd



# Carrega as variáveis de ambiente
load_dotenv()

# Contexto para funcionamento da instância
ADMIN_NUMBER = os.getenv('ADMIN_NUMBER')

NOME_ASSISTENTE = os.getenv("NOME_ASSISTENTE", None)

BUSINESS_CONTEXT = ""

FLOW_TO_INTENTION_MAP = {'aniversarios': 'Indefinido'}

SPECIFIC_INFORMATION_LIST = []

client = openai.OpenAI(
    api_key=os.getenv("MARITACA_API_KEY", None),  # Substitua pela sua API Key
    base_url="https://chat.maritaca.ai/api",
)


# Funções para o Apscheduler
def start_apscheduler():
    """Inicia o scheduler de tarefas"""
    
    scheduler = BackgroundScheduler(timezone='America/Sao_Paulo')
    
    try:
        if not scheduler.running:
            scheduler.start()
            print("🚀 Scheduler iniciado com sucesso!")
            
            # ✅ ADICIONAR ESTA LINHA:
            return scheduler  # ← Para poder salvar no app.state
            
    except Exception as e:
        print(f"❌ Erro ao iniciar scheduler: {str(e)}")
        return None

# Validação de segurança
if not ADMIN_NUMBER:
    print("⚠️ AVISO: ADMIN_NUMBER não configurado no .env - alertas de erro não funcionarão!")
    ADMIN_NUMBER = None

def generate_uuid():
    """
    Gera um UUID (Universally Unique Identifier) no formato padrão.
    Retorna:
        str: Uma string representando o UUID gerado.
    """
    novo_uuid = uuid.uuid4()  # Gera um UUID aleatório
    return str(novo_uuid)

# Funções para os envios de mensagens (auxiliares ou funções diretas)

async def send_ai_message(
    phone_number: str,  # Mudado de int para str
    message: str,
    http_client: aiohttp.ClientSession
) -> Dict[str, Any]:

    instance_name = os.getenv("WHATSAPP_API_INSTANCE")
    api_key = os.getenv("WHATSAPP_API_TOKEN")
    push_name = os.getenv("AGENT_NAME")
    url = os.getenv("CHAT_SEND_ENDPOINT")

    async def send_track_message(
    phone_number: str,  # Mudado de int para str
    message: str, 
    instance_name: str, 
    api_key: str, 
    message_type: str, 
    push_name: str, 
    url: str,
    http_client: aiohttp.ClientSession
) -> Dict[str, Any]:
        
        send_url = url
        
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json'
        }
        
        data = {
            "instance_name": instance_name,
            "evolution_api_key": api_key,
            "phone_number": phone_number,
            "message": message,
            "type": message_type,
            "push_name": push_name
        }
        
        try:
            # Use o cliente compartilhado
            async with http_client.post(send_url, headers=headers, json=data) as response:
                response.raise_for_status()
                return {
                    "status_code": response.status,
                    "response": await response.json()
                }
        except aiohttp.ClientResponseError as e:
            return {
                "error": f"Erro HTTP: {e.status} - {str(e)}"
            }
        except Exception as e:
            return {
                "error": f"Erro de conexão: {str(e)}"
            }

    # Verificar se todos os parâmetros necessários estão disponíveis
    if not all([instance_name, api_key, phone_number, message, url]):
        missing = []
        if not instance_name: missing.append("WHATSAPP_API_INSTANCE")
        if not api_key: missing.append("WHATSAPP_API_TOKEN")
        if not push_name: missing.append("AGENT_NAME")
        if not url: missing.append("CHAT_SEND_ENDPOINT")
        if not phone_number: missing.append("phone_number")
        if not message: missing.append("message")
        
        error_msg = f"Parâmetros ausentes: {', '.join(missing)}"
        print(f"⭐ ERRO em send_ai_message: {error_msg}")
        return {"error": error_msg}
    
    try:
        result = await send_track_message(
            phone_number=phone_number,
            message=message,
            instance_name=os.getenv("WHATSAPP_API_INSTANCE"),      
            api_key=os.getenv("WHATSAPP_API_TOKEN"),           
            message_type="AIMessage",
            push_name=os.getenv("AGENT_NAME"),
            url=os.getenv("CHAT_SEND_ENDPOINT"),
            http_client=http_client  # Passe o cliente HTTP
        )
        return result
    except Exception as e:
        print(f"⭐ ERRO em send_ai_message: {e}")
        return {"error": str(e)}

async def send_system_message(
    phone_number: str,
    message: str,
    http_client: aiohttp.ClientSession
) -> Dict[str, Any]:
    """
    Envia SystemMessage direto para o webhook interno (não para WhatsApp)
    Evita loop infinito indo direto para o webhook ao invés do /send
    """
    
    # Usar o endpoint do webhook diretamente
    base_url = os.getenv("CHAT_SEND_ENDPOINT", "").replace("/send", "")
    webhook_url = f"{base_url}/webhook"
    
    # Ou usar variável específica se preferir
    # webhook_url = os.getenv("WEBHOOK_INTERNAL_ENDPOINT", "http://localhost:8000/webhook")
    
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    # Simular payload do webhook para SystemMessage
    # Formato idêntico ao que o WhatsApp enviaria
    data = {
        "event": "send.message",  # Simular evento de envio do sistema
        "instance": os.getenv("WHATSAPP_API_INSTANCE"),
        "data": {
            "pushName": "Sistema",  # Nome fixo para SystemMessages
            "key": {
                "remoteJid": f"{phone_number}@s.whatsapp.net",
                "fromMe": True  # True porque é mensagem do sistema
            },
            "message": {
                "conversation": message,
                "type": "SystemMessage"  # Identificador adicional
            }
        },
        "type": "SystemMessage"  # Identificador no nível raiz do payload
    }
    
    print(f"[send_system_message] 📤 Enviando para webhook: {webhook_url}")
    print(f"[send_system_message] 📱 Phone: {phone_number}")
    print(f"[send_system_message] 💬 Message: {message[:50]}...")
    
    try:
        print(f"[SEND-SYSTEM-DEBUG] 📦 Payload COMPLETO que será enviado:")
        print(f"[SEND-SYSTEM-DEBUG] {json.dumps(data, indent=2)}")
        async with http_client.post(webhook_url, headers=headers, json=data) as response:
            response.raise_for_status()
            result = await response.json()
            
            print(f"[send_system_message] ✅ SystemMessage enviada com sucesso")
            return {
                "status_code": response.status,
                "response": result,
                "message_type": "SystemMessage",
                "sent_to_webhook": True
            }
            
    except aiohttp.ClientResponseError as e:
        error_msg = f"Erro HTTP no webhook: {e.status} - {str(e)}"
        print(f"[send_system_message] ❌ {error_msg}")
        return {"error": error_msg}
        
    except Exception as e:
        error_msg = f"Erro ao enviar SystemMessage para webhook: {str(e)}"
        print(f"[send_system_message] ❌ {error_msg}")
        return {"error": error_msg}

def send_base64_image(url_api_whatsapp, instancia, api_key, numero, caminho_imagem, legenda="", delay=0, presence="composing"):
    """
    Envia uma imagem via EvolutionAPI usando base64
    
    Args:
        servidor (str): URL do servidor EvolutionAPI
        instancia (str): Nome da instância configurada
        api_key (str): Chave de API para autenticação
        numero (str): Número do destinatário com código do país
        caminho_imagem (str): Caminho local ou URL da imagem
        legenda (str, opcional): Legenda da imagem
        delay (int, opcional): Atraso em milissegundos
        presence (str, opcional): Status de presença (composing, recording)
        
    Returns:
        dict: Resposta da API
    """
    # Configura a URL e cabeçalhos
    url = f"{url_api_whatsapp}/message/sendMedia/{instancia}"
    headers = {
        "Content-Type": "application/json",
        "apikey": api_key
    }
    
    # Determina se o caminho é uma URL ou arquivo local
    if caminho_imagem.startswith('http'):
        # Baixa a imagem da URL
        try:
            print(f"Baixando imagem da URL: {caminho_imagem}")
            response = requests.get(caminho_imagem, timeout=10)
            if response.status_code != 200:
                print(f"Erro ao baixar imagem: {response.status_code}")
                return None
            imagem_binaria = response.content
            print(f"Imagem baixada com sucesso: {len(imagem_binaria)} bytes")
        except Exception as e:
            print(f"Exceção ao baixar imagem: {str(e)}")
            return None
    else:
        # Lê a imagem do arquivo local
        try:
            print(f"Carregando imagem do arquivo local: {caminho_imagem}")
            with open(caminho_imagem, 'rb') as file:
                imagem_binaria = file.read()
            print(f"Imagem carregada com sucesso: {len(imagem_binaria)} bytes")
        except Exception as e:
            print(f"Erro ao ler arquivo de imagem: {str(e)}")
            return None
    
    # Converte para base64
    try:
        b64_imagem = base64.b64encode(imagem_binaria).decode('utf-8')
    except Exception as e:
        print(f"Erro ao converter para base64: {str(e)}")
        return None
    
    # Prepara os dados - envia apenas a string base64
    payload = {
        "number": numero,
        "mediatype": "image",
        "media": b64_imagem,  # Não imprimimos esta variável em logs
        "caption": legenda,
        "delay": delay,
        "presence": presence,
    }
    
    # Log simplificado do payload, omitindo o conteúdo base64
    log_payload = payload.copy()
    if "media" in log_payload:
        log_payload["media"] = f"[BASE64_STRING_{len(b64_imagem)}_chars]"
    print(f"Enviando payload: {log_payload}")
    
    # Realiza a requisição
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    
    # Verifica se a requisição foi bem-sucedida
    if response.status_code == 200 or response.status_code == 201:
        print(f"Imagem enviada com sucesso para {numero}")
        return response.json()
    else:
        print(f"Erro ao enviar imagem: {response.status_code}")
        print(response.text)
        return None

async def send_base64_image_async(
    url_api_whatsapp: str, 
    instancia: str, 
    api_key: str, 
    numero: str, 
    caminho_imagem: str, 
    legenda: str = "", 
    delay: int = 0, 
    presence: str = "composing",
    http_client: Optional[aiohttp.ClientSession] = None,
    # 🆕 PARÂMETROS PARA FLUXO DE ATIVAÇÃO (igual ao /send)
    app_state = None,
    is_activation_flow: bool = False,
    flow_name: str = None
) -> Optional[dict]:
    """
    Versão ASYNC da função de envio de imagem com suporte a fluxo de ativação
    
    SEGUE A MESMA LÓGICA DO /send:
    1. Detecta se é fluxo de ativação
    2. Salva no cache antes de enviar
    3. Envia a imagem
    4. Webhook detecta pelo cache
    """
    
    print(f"[ASYNC_IMAGE] 📸 Iniciando envio de imagem")
    print(f"[ASYNC_IMAGE] 📱 Número: {numero}")
    print(f"[ASYNC_IMAGE] 🖼️ Caminho: {caminho_imagem}")
    print(f"[ASYNC_IMAGE] 🎯 Tipo de fluxo: {'ATIVAÇÃO' if is_activation_flow else 'NORMAL'}")
    
    # ===== LÓGICA DE ATIVAÇÃO (IGUAL AO /send) =====
    if is_activation_flow:
        print(f"[ASYNC_IMAGE] 📋 Flow Name: {flow_name}")
        
        if app_state:
            # 🌌 CACHE PARA DOMINAÇÃO GALÁCTICA (igual ao /send)
            cache_key = f"{numero}_{int(time.time())}"
            app_state.activation_cache[cache_key] = flow_name
            print(f"[ASYNC_IMAGE] 🎯 Cache: {cache_key} = {flow_name}")
        else:
            print(f"[ASYNC_IMAGE] ⚠️ app_state não fornecido - cache não salvo")
    
    # Configura a URL e cabeçalhos
    base_url = url_api_whatsapp.replace(f"/message/sendText/{instancia}", "")
    url = f"{base_url}/message/sendMedia/{instancia}"
    headers = {
        "Content-Type": "application/json",
        "apikey": api_key
    }
    
    # Verificar se temos um cliente HTTP, senão criar um temporário
    usar_cliente_temporario = http_client is None
    if usar_cliente_temporario:
        print(f"[ASYNC_IMAGE] ⚠️ Criando cliente HTTP temporário")
        http_client = aiohttp.ClientSession()
    
    try:
        # ===== DOWNLOAD/CARREGAMENTO DA IMAGEM =====
        if caminho_imagem.startswith('http'):
            # Baixa a imagem da URL
            print(f"[ASYNC_IMAGE] 🌐 Baixando imagem da URL: {caminho_imagem}")
            
            try:
                async with http_client.get(caminho_imagem, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status != 200:
                        print(f"[ASYNC_IMAGE] ❌ Erro ao baixar imagem: HTTP {response.status}")
                        return None
                    
                    imagem_binaria = await response.read()
                    print(f"[ASYNC_IMAGE] ✅ Imagem baixada: {len(imagem_binaria)} bytes")
                    
            except Exception as e:
                print(f"[ASYNC_IMAGE] ❌ Exceção ao baixar imagem: {str(e)}")
                return None
        else:
            # Lê a imagem do arquivo local
            print(f"[ASYNC_IMAGE] 📁 Carregando imagem do arquivo local: {caminho_imagem}")
            
            try:
                with open(caminho_imagem, 'rb') as file:
                    imagem_binaria = file.read()
                print(f"[ASYNC_IMAGE] ✅ Imagem carregada: {len(imagem_binaria)} bytes")
            except Exception as e:
                print(f"[ASYNC_IMAGE] ❌ Erro ao ler arquivo: {str(e)}")
                return None
        
        # ===== CONVERSÃO PARA BASE64 =====
        try:
            b64_imagem = base64.b64encode(imagem_binaria).decode('utf-8')
            print(f"[ASYNC_IMAGE] 🔐 Base64 gerado: {len(b64_imagem)} caracteres")
        except Exception as e:
            print(f"[ASYNC_IMAGE] ❌ Erro ao converter para base64: {str(e)}")
            return None
        
        # ===== PREPARAR PAYLOAD =====
        payload = {
            "number": numero,
            "mediatype": "image",
            "media": b64_imagem,
            "caption": legenda,
            "delay": delay,
            "presence": presence,
        }
        
        # Log simplificado do payload
        log_payload = payload.copy()
        log_payload["media"] = f"[BASE64_STRING_{len(b64_imagem)}_chars]"
        print(f"[ASYNC_IMAGE] 📦 Payload preparado: {log_payload}")
        
        # ===== ENVIO DA IMAGEM =====
        print(f"[ASYNC_IMAGE] 🚀 Enviando para: {url}")
        
        try:
            async with http_client.post(
                url, 
                headers=headers, 
                data=json.dumps(payload),
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                
                response_status = response.status
                print(f"[ASYNC_IMAGE] 📡 Status da resposta: {response_status}")
                
                # Verifica se a requisição foi bem-sucedida
                if response_status in [200, 201]:
                    try:
                        response_data = await response.json()
                        print(f"[ASYNC_IMAGE] ✅ Imagem enviada com sucesso para {numero}")
                        
                        # ===== LOG PARA FLUXO DE ATIVAÇÃO =====
                        if is_activation_flow:
                            print(f"[ASYNC_IMAGE] 🎯 Imagem de ativação enviada")
                            print(f"[ASYNC_IMAGE] 📋 Flow: {flow_name}")
                            print(f"[ASYNC_IMAGE] 📝 Legenda: {legenda[:50]}...")
                        
                        return response_data
                        
                    except json.JSONDecodeError:
                        # Fallback se não for JSON válido
                        response_text = await response.text()
                        print(f"[ASYNC_IMAGE] ✅ Imagem enviada (resposta não-JSON): {response_text}")
                        return {"status": "success", "response": response_text}
                else:
                    response_text = await response.text()
                    print(f"[ASYNC_IMAGE] ❌ Erro ao enviar imagem: {response_status}")
                    print(f"[ASYNC_IMAGE] ❌ Resposta: {response_text}")
                    return None
                    
        except Exception as e:
            print(f"[ASYNC_IMAGE] ❌ Exceção no envio: {str(e)}")
            return None
    
    finally:
        # Fechar cliente temporário se foi criado aqui
        if usar_cliente_temporario:
            await http_client.close()
            print(f"[ASYNC_IMAGE] 🔐 Cliente HTTP temporário fechado")

async def send_presence(number: str, 
                       delay: int = 1200, 
                       presence: str = "composing",
                       base_url: Optional[str] = None,
                       api_key: Optional[str] = None,
                       instance: Optional[str] = None) -> Dict[Any, Any]:
    """
    Envia presença (status de digitação, gravação, etc.) para um contato via Evolution API
    
    Args:
        number (str): Número do telefone (formato: 5531999999999)
        delay (int): Tempo em milissegundos para manter a presença (padrão: 1200)
        presence (str): Tipo de presença - opções:
                       - "composing" (digitando)
                       - "recording" (gravando áudio)
                       - "paused" (pausado)
        base_url (str, optional): URL base da API (padrão: EVOLUTION_URL env var)
        api_key (str, optional): Chave da API (padrão: EVOLUTION_API_KEY env var)
        instance (str, optional): ID da instância (padrão: WHATSAPP_API_INSTANCE env var)
    
    Returns:
        Dict[Any, Any]: Resposta da API
    
    Raises:
        aiohttp.ClientError: Erro na requisição HTTP
        ValueError: Erro nos parâmetros fornecidos
    """
    
    # Usa variáveis de ambiente como padrão
    base_url = base_url or os.getenv('EVOLUTION_URL')
    api_key = api_key or os.getenv('EVOLUTION_API_KEY')
    instance = instance or os.getenv('WHATSAPP_API_INSTANCE')
    
    # Validação básica dos parâmetros
    if not all([base_url, api_key, instance, number]):
        raise ValueError("Parâmetros obrigatórios não fornecidos. Verifique as variáveis de ambiente EVOLUTION_URL, EVOLUTION_API_KEY e WHATSAPP_API_INSTANCE")
    
    # Monta a URL do endpoint
    url = f"{base_url.rstrip('/')}/chat/sendPresence/{instance}"
    
    # Headers da requisição
    headers = {
        'Content-Type': 'application/json',
        'apikey': api_key
    }
    
    # Corpo da requisição
    payload = {
        "number": number,
        "delay": delay,
        "presence": presence
    }
    
    # Timeout configurado
    timeout = aiohttp.ClientTimeout(total=30)
    
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, json=payload) as response:
                
                # Verifica se a requisição foi bem-sucedida
                if response.status >= 400:
                    error_text = await response.text()
                    raise aiohttp.ClientError(f"Erro HTTP {response.status}: {error_text}")
                
                # Retorna a resposta como JSON
                return await response.json()
                
    except asyncio.TimeoutError:
        raise aiohttp.ClientError("Timeout: A requisição demorou muito para responder")
    except aiohttp.ClientConnectorError:
        raise aiohttp.ClientError("Erro de conexão com a API")
    except json.JSONDecodeError:
        raise aiohttp.ClientError("Erro ao decodificar a resposta JSON da API")


# Funções para definições dos grafos

def create_new_thread_graph_logging(http_client: aiohttp.ClientSession,
                            app_state: Any):

    class InputState(TypedDict):
        messages: Annotated[list, add_messages]
        name: str
        phone_number: str  # Mudado de int para str
        start_time: int
        thread_id: str
        message_timestamps: Dict[str, int]
        need_classify: bool
        structured_response: Optional[Dict[str, Any]]
        
    class GeneralState(TypedDict):
        messages: Annotated[list, add_messages]
        name: str
        phone_number: str  # Mudado de int para str
        status: str
        start_time: int
        last_update: str
        thread_id: str
        messages_count: int
        collected_data: Dict
        message_timestamps: Dict[str, int]
        classify_status: str
        need_classify: bool
        structured_response: Optional[Dict[str, Any]]
    
    async def start_or_update(state: InputState) -> InputState:
            """
            Nó inicial do grafo que configura ou atualiza o estado conforme necessário.
            
            Args:
                state (InputState): O estado atual com os dados recebidos
                
            Returns:
                InputState: O estado atualizado com os campos inicializados
            """

            # Capturar timestamp atual uma vez
            current_time = int(time.time() * 1000)
            
            # ✅ DEBUG ADICIONAL: Ver o que está chegando
            #print(f"[DEBUG-START] message_timestamps RECEBIDO: {state.get('message_timestamps', 'CAMPO_NAO_EXISTE')}")
            #print(f"[DEBUG-START] 🔧 ESTADO COMPLETO RECEBIDO: {list(state.keys())}")
            #print(f"[DEBUG-START] 🔧 classify_status no InputState: {state.get('classify_status', 'CAMPO_NAO_EXISTE')}")

            # Garantir que temos um campo 'messages' no estado
            if 'messages' not in state:
                state['messages'] = []
            
            # Verificar se é a primeira execução (sem start_time)
            if 'start_time' not in state:
                state['start_time'] = current_time
            
            # Inicializar dados coletados
            if 'collected_data' not in state:
                state['collected_data'] = {}
            
            # ✅ DEBUG: Verificar se message_timestamps existe
            if 'message_timestamps' not in state:
                #print(f"[DEBUG-START] Criando message_timestamps vazio")
                state['message_timestamps'] = {}
            else:
                print(f"[DEBUG-START] message_timestamps já existe: {state['message_timestamps']}")
            
            # Debug para entender o que está acontecendo
            #print(f"[DEBUG-START] Total de mensagens: {len(state['messages'])}")
            #print(f"[DEBUG-START] Timestamps existentes: {len(state.get('message_timestamps', {}))}")
            
            # ✅ VERIFICAR SE HÁ MENSAGENS NOVAS PARA ADICIONAR TIMESTAMP
            new_messages_added = False
            
            # Só adicionar timestamp para mensagens REALMENTE novas
            for message in state['messages']:
                message_id = message.id
                message_exists = message_id in state['message_timestamps']
                
                #print(f"[DEBUG-START] Mensagem {message_id[:8]}... já existe? {message_exists}")
                
                # Só adiciona timestamp se a mensagem for realmente nova
                if not message_exists:
                    state['message_timestamps'][message_id] = current_time
                    new_messages_added = True
                    #print(f"[DEBUG-START] ✅ Adicionado timestamp para {message_id[:8]}...")
                else:
                    print(f"[DEBUG-START] ⏭️ Pulando {message_id[:8]}... (já tem timestamp)")
            
            # Atualizar o contador de mensagens
            state['messages_count'] = len(state['messages'])
            
            # Sempre atualiza o timestamp de última atualização
            state['last_update'] = current_time

            # ✅ VERIFICAÇÃO INTELIGENTE DO STATUS - CORRIGIDA PARA REABERTURA
            # 🔧 CORREÇÃO: Verificar o status nos InputState primeiro, senão pegar do estado existente
            current_classify_status = state.get('classify_status', None)
            
            # 🔧 ADICIONAR DEBUG DETALHADO
            #print(f"[DEBUG-START] Status da classificação recebido: {current_classify_status}")
            #print(f"[DEBUG-START] Novas mensagens adicionadas? {new_messages_added}")
            #print(f"[DEBUG-START] Total de mensagens no estado: {len(state.get('messages', []))}")
            
            # 🔧 LÓGICA CORRIGIDA: Determinar o status baseado na situação atual
            if current_classify_status is None:
                # Primeira execução - nova conversa
                state['classify_status'] = 'opened'
                #print(f"[DEBUG-START] 🆕 NOVA CONVERSA - Definindo como 'opened'")
            elif current_classify_status == 'classified' and new_messages_added:
                # 🎯 REABERTURA: Conversa classificada recebendo nova mensagem
                state['classify_status'] = 'opened'
                #print(f"[DEBUG-START] 🔄 REABRINDO CONVERSA - Era 'classified', nova mensagem recebida → 'opened'")
            elif current_classify_status == 'opened' and new_messages_added:
                # Conversa já aberta recebendo nova mensagem - mantém aberta
                state['classify_status'] = 'opened'
                #print(f"[DEBUG-START] ➡️ CONTINUANDO CONVERSA ABERTA - Nova mensagem → mantém 'opened'")
            elif current_classify_status == 'opened' and not new_messages_added:
                # Execução sem mensagens novas em conversa aberta - mantém aberta
                state['classify_status'] = 'opened'
                #print(f"[DEBUG-START] ➡️ EXECUTANDO SEM NOVA MENSAGEM - Mantém 'opened'")
            elif current_classify_status == 'classified' and not new_messages_added:
                # Execução sem mensagens novas em conversa classificada - mantém classificada
                state['classify_status'] = 'classified'
                #print(f"[DEBUG-START] 📋 MANTENDO CLASSIFIED - Sem mensagens novas → mantém 'classified'")
            else:
                # Fallback - força abertura
                state['classify_status'] = 'opened'
                print(f"[DEBUG-START] ❓ FALLBACK - Status '{current_classify_status}' → forçando 'opened'")
            
            print(f"[DEBUG-START] Status final da classificação: {state['classify_status']}")
            #print(f"[DEBUG-START] Timestamps finais: {len(state['message_timestamps'])}")
            #print(f"[DEBUG-START] IDs dos timestamps: {list(state['message_timestamps'].keys())}")
            
            return state

    async def check_combined_messages_node(state: GeneralState) -> GeneralState:
        """
        Nó que verifica se existem mensagens combinadas pendentes.
        
        Dois cenários:
        1. Se detectar que a mensagem atual faz parte de uma mensagem combinada,
        interrompe o processamento.
        2. Se a mensagem atual é uma mensagem combinada completa, substitui a mensagem
        parcial anterior no histórico.
        """
        
        # 🔍 ADICIONAR ESTE DEBUG AQUI:
        print(f"🔍 LOGGING GRAPH DEBUG: State keys: {list(state.keys())}")
        print(f"🔍 LOGGING GRAPH DEBUG: Phone presente: {'phone_number' in state}")
        print(f"🔍 LOGGING GRAPH DEBUG: State completo: {state}")

        phone_number = state.get("phone_number", "PHONE_NOT_FOUND")
        print(f"🔍 Phone extraído: {phone_number}")
        
        if phone_number == "PHONE_NOT_FOUND":
            print(f"🚨 ERRO: phone_number não encontrado no estado!")
            return state  # Retorna sem processar


        # Garantir que temos mensagens no histórico
        if not state.get("messages") or len(state["messages"]) == 0:
            return state
        
        current_message = state["messages"][-1].content
        
        # Verificar se existe mensagem combinada para este número
        has_combined = False
        is_combined_message = False  # Flag para indicar se esta é a mensagem combinada completa
        combined_message = None
        
        if hasattr(app_state, 'combined_messages_status') and phone_number in app_state.combined_messages_status:
            combined_status = app_state.combined_messages_status[phone_number]
            has_combined = combined_status.get("has_combined_messages", False)
            combined_message = combined_status.get("combined_message", "")
            
            # Determinar se esta é a mensagem combinada completa
            is_combined_message = has_combined and current_message == combined_message
        
        # Cenário 1: Esta é uma mensagem parcial que será combinada
        if has_combined and not is_combined_message:
            print(f"[DEBUG-VERIFY] ⛔ INTERROMPENDO - Mensagem parcial detectada: '{current_message}'")
            print(f"[DEBUG-VERIFY] 🔄 Será parte da mensagem combinada: '{combined_message}'")
            
            # Interromper o processamento - não precisamos salvar estado
            # porque esta mensagem parcial será substituída na próxima execução
            raise NodeInterrupt("partial_message_detected")
        
        # Cenário 2: Esta é a mensagem combinada completa
        if is_combined_message:
            print(f"[DEBUG-VERIFY] 🔄 Processando mensagem combinada: '{combined_message}'")
            
            # Verificar se temos pelo menos duas mensagens no histórico
            if len(state["messages"]) >= 2:
                # Obter a penúltima mensagem (a mensagem parcial)
                prev_message = state["messages"][-2].content
                
                # Verificar se a mensagem combinada inclui a mensagem parcial anterior
                if combined_message.startswith(prev_message + " "):
                    print(f"[DEBUG-VERIFY] 🔍 Mensagem parcial anterior: '{prev_message}'")
                    
                    # ✅ ADICIONAR AQUI: Obter o ID da mensagem parcial antes de removê-la
                    removed_message_id = state["messages"][-2].id
                    
                    # Remover a mensagem parcial anterior do histórico
                    state["messages"].pop(-2)
                    
                    # ✅ ADICIONAR AQUI: Remover o timestamp da mensagem parcial também
                    if removed_message_id in state.get("message_timestamps", {}):
                        del state["message_timestamps"][removed_message_id]
                        print(f"[DEBUG-VERIFY] 🗑️ Removido timestamp da mensagem parcial: {removed_message_id}")
                    
                    # Atualizar o contador de mensagens
                    state["messages_count"] = len(state["messages"])
                    
                    print(f"[DEBUG-VERIFY] ✅ Removida mensagem parcial do histórico")
                    print(f"[DEBUG-VERIFY] 📊 Histórico final: {[m.content for m in state['messages']]}")
        
        # Para qualquer outro caso, continuar normalmente
        return state

    async def check_conversation_status(state: GeneralState) -> GeneralState:
        """
        Determina se a conversa está aberta e define o status de classificação.
        """
        current_time = int(time.time() * 1000)
        message_timestamps = state.get('message_timestamps', {})
        
        need_classify = state.get('need_classify', False)
        print(f"Need Classify?: {need_classify}")

        if not message_timestamps:
            # Se não há timestamps, considerar como classificada
            state['classify_status'] = 'classified'
            return state
        
        # Pegar o timestamp mais recente
        latest_timestamp = max(message_timestamps.values())
        
        # Calcular diferença (1 hora = 3600000 ms)
        time_diff = current_time - latest_timestamp
        ONE_HOUR_MS = 3600000
        
        # Determinar status
        if time_diff < ONE_HOUR_MS:
            state['classify_status'] = 'opened'
        else:
            state['classify_status'] = 'classified'
        
        return state

    async def classify_conversation(state: GeneralState) -> GeneralState:
        """
        Nó que classifica conversa apenas quando necessário.
        
        Este nó sempre é executado no fluxo, mas só efetivamente classifica
        a conversa quando a flag need_classify é True (indicando que foi
        executado via time travel para classificação).
        
        Args:
            state (GeneralState): O estado atual da conversa
            
        Returns:
            GeneralState: O estado atualizado (com classify_status = 'classified' se necessário)
        """
        need_classify = state.get('need_classify', False)
        thread_id = state.get('thread_id', 'unknown')
        phone_number = state.get('phone_number', 'unknown')
        
        if need_classify:
            # Classificar a conversa
            state['classify_status'] = 'classified'
            
            print(f"[CLASSIFY] 🎯 Conversa classificada!")
            print(f"[CLASSIFY] 📱 Phone: {phone_number}")
            print(f"[CLASSIFY] 🧵 Thread: {thread_id}")
            print(f"[CLASSIFY] 🏷️ Status: opened → classified")
            
            # Log adicional para debug
            messages_count = state.get('messages_count', 0)
            print(f"[CLASSIFY] 📊 Total de mensagens na conversa: {messages_count}")
            
        else:
            # Execução normal - não precisa classificar
            print(f"[CLASSIFY] ⏭️ Execução normal - need_classify=False")
            print(f"[CLASSIFY] 📱 Phone: {phone_number}")
            print(f"[CLASSIFY] 🏷️ Status mantido: {state.get('classify_status', 'unknown')}")
        
        return state

    async def log_information(state: GeneralState) -> GeneralState:

        async def upsert_graph_info(
            phone_number: str, 
            data: Dict[str, Any], 
            http_client: aiohttp.ClientSession,  # Adicione o parâmetro
            base_row_api_token: str = os.getenv("BASEROW_API_TOKEN"),
            table_id: int = os.getenv("BASEROW_GRAPH_TABLE_ID"),
            base_url: str = os.getenv("BASEROW_URL")
        ) -> Dict[str, Any]:
        
            # Verificação de parâmetros
            if not base_row_api_token:
                raise ValueError("O token de API é obrigatório")
            
            if not phone_number:
                raise ValueError("O número de telefone é obrigatório para o upsert")
            
            if not data or not isinstance(data, dict):
                raise ValueError("Os dados devem ser fornecidos como um dicionário")
            
            # Configurar os cabeçalhos
            headers = {
                "Authorization": f"Token {base_row_api_token.strip()}",
                "Content-Type": "application/json"
            }
            
            start_time_func = time.time()
            
            # Preparar dados de atualização com os nomes corretos dos campos
            update_data = {
                'phoneNumber': data.get('phoneNumber', phone_number),
                'thread_id': data.get('thread_id'),
                'start_timeTimestamp': data.get('start_time', data.get('start_timeTimestamp')),
                'last_updateTimestamp': data.get('last_update', data.get('last_updateTimestamp')),
                'intention': data.get('intention'),
                'collected_data': data.get('collected_data', '{}'),
                'message_timestamps': data.get('message_timestamps', {}),
                'messages_count': data.get('messages_count'),
                'classify_status': data.get('classify_status', 'opened')
            }
            
            # Remover chaves com valor None
            update_data = {k: v for k, v in update_data.items() if v is not None}
            
            try:
                # Buscar se o registro já existe usando o número de telefone
                search_url = f"{base_url}/api/database/rows/table/{table_id}/?user_field_names=true&filter__phoneNumber__equal={phone_number}&filter__thread_id__equal={data.get('thread_id')}"
                
                async with http_client.get(search_url, headers=headers) as search_response:
                    search_response.raise_for_status()
                    search_result = await search_response.json()
                
                # Verificar se o registro existe
                if search_result.get('count', 0) > 0:
                    # Registro existe, atualizar
                    record_id = search_result['results'][0]['id']
                    update_url = f"{base_url}/api/database/rows/table/{table_id}/{record_id}/?user_field_names=true"
                    
                    async with http_client.patch(update_url, headers=headers, json=update_data) as response:
                        response.raise_for_status()
                        result = await response.json()
                        operation = "atualizado"
                else:
                    # Registro não existe, inserir
                    insert_url = f"{base_url}/api/database/rows/table/{table_id}/?user_field_names=true"
                    
                    async with http_client.post(insert_url, headers=headers, json=update_data) as response:
                        response.raise_for_status()
                        result = await response.json()
                        operation = "inserido"
                
                end_time = time.time()
                processing_time = round((end_time - start_time_func) * 1000, 2)
                print(f"Registro {operation} para {phone_number} em {processing_time:.2f} ms")
                
                # Adicionar tempo de processamento e operação ao resultado
                if isinstance(result, dict):
                    result['processing_time_ms'] = processing_time
                    result['operation'] = operation
                    result['phone_number'] = phone_number
                
                return result
            
            except aiohttp.ClientResponseError as e:
                print(f"Erro HTTP ao realizar upsert: {e}")
                if hasattr(e, 'response') and hasattr(e.response, 'text'):
                    response_text = await e.response.text()
                    print(f"Detalhes do erro: {response_text}")
                raise
            
            except Exception as e:
                print(f"Erro inesperado ao realizar upsert: {e}")
                raise               
        
        state['last_update'] = int(time.time()*1000)

        # Extrair todos os valores do estado no início
        phone_number = state["phone_number"]
        thread_id = state["thread_id"]
        start_time = state.get("start_time")
        last_update = state['last_update']
        collected_data = state.get('collected_data', {})
        messages_count = state.get('messages_count', 0)
        message_timestamps = state.get('message_timestamps', {})
        classify_status = state.get('classify_status', 'opened')

        # Montar o objeto de dados de forma limpa
        upsert_data = {
            'phoneNumber': phone_number,
            'thread_id': thread_id,
            'start_timeTimestamp': start_time,
            'last_updateTimestamp': last_update,
            'collected_data': str(collected_data),
            'messages_count': messages_count,
            'message_timestamps': str(message_timestamps),
            'classify_status': classify_status
        }
        
        print(f"Dados que estamos enviando para serem inseridos na tabela de grafos {upsert_data}")
        
        try:
            print(f"Chamando upsert_graph_info para phone_number: {phone_number}")
            resultado = await upsert_graph_info(
                phone_number=phone_number,
                data=upsert_data, 
                http_client=http_client
            )
            print(f"Resultado do upsert_graph_info: {json.dumps(resultado, indent=2)}")
        except Exception as e:
            print(f"ERRO em upsert_graph_info: {e}")
            print(f"Traceback: {traceback.format_exc()}")
        
        print("==== FINALIZANDO NÓ LOG_INFORMATION ====\n")
        
        return state

    async def log_messages(state: GeneralState) -> GeneralState:
        """
        🔧 VERSÃO CORRIGIDA SIMPLES: Usa sempre o pushName original do webhook
        """
        
        async def upsert_message(
    phone_number: str, 
    message: str, 
    message_type: str,
    event: str,
    http_client: aiohttp.ClientSession,  # Cliente HTTP compartilhado
    push_name: str = None,  # 🆕 Novo parâmetro para pushName correto
    base_row_api_token: str = os.getenv("BASEROW_API_TOKEN"),
    table_id: int = int(os.getenv("BASEROW_MESSAGE_TABLE_ID")),
    base_url: str = os.getenv("BASEROW_URL")
):
            """
            Função para registrar mensagens diretamente no Baserow sem enviar para o webhook.
            🔧 CORRIGIDA: Agora aceita pushName como parâmetro para usar o nome correto
            
            Args:
                phone_number (str): Número de telefone associado à mensagem
                message (str): Conteúdo da mensagem
                message_type (str): Tipo da mensagem (ex: "SystemMessage", "HumanMessage", "AIMessage")
                http_client (aiohttp.ClientSession): Cliente HTTP compartilhado
                push_name (str): Nome do remetente (se None, usa AGENT_NAME como fallback)
                base_row_api_token (str): Token de API do Baserow
                table_id (int): ID da tabela no Baserow
                base_url (str): URL base do Baserow
                
            Returns:
                Dict[str, Any]: Resultado da operação no Baserow
            """
            # Verificação de parâmetros
            if not all([phone_number, message, message_type, event]):
                raise ValueError("Todos os parâmetros são obrigatórios: phone_number, message, message_type, event")
            
            headers = {
                "Authorization": f"Token {base_row_api_token.strip()}",
                "Content-Type": "application/json",
                "Connection": "close",
            }
            
            # 🔧 CORREÇÃO: Determinar pushName correto
            if push_name:
                sender_name = push_name
                print(f"[LOG-BASEROW] ✅ Usando pushName fornecido: '{sender_name}'")
            else:
                sender_name = os.getenv('AGENT_NAME', 'Sistema')
                print(f"[LOG-BASEROW] ⚠️ PushName não fornecido, usando fallback: '{sender_name}'")

            
            print(f"[DEBUG-EVENT TYPE] ⚠️ Será usado o Event: '{event}'")
            
            
            # Preparar dados para o Baserow
            record_data = {
                "event": event,  # Tipo do evento
                "instance": os.getenv('WHATSAPP_API_INSTANCE'),  # Nome da instância
                "pushName": sender_name,  # 🔧 CORRIGIDO: Nome do remetente correto
                "message": message,  # Conteúdo da mensagem
                "messageTimestamp": int(time.time() * 1000),  # Timestamp atual
                "phoneNumber": phone_number,  # Número de telefone
                "type": message_type  # Tipo de mensagem
            }
            
            print(f"[LOG-BASEROW] 📝 Dados preparados:")
            print(f"[LOG-BASEROW]   Event: '{event}'")
            print(f"[LOG-BASEROW]   PushName: '{sender_name}'")
            print(f"[LOG-BASEROW]   Message: '{message[:50]}...'")
            print(f"[LOG-BASEROW]   Type: '{message_type}'")
            print(f"[LOG-BASEROW]   Phone: '{phone_number}'")
            
            print(f"Enviando para Baserow - Dados: {record_data}")
            
            # Definir a URL antes do bloco try
            url = f"{base_url}/api/database/rows/table/{table_id}/?user_field_names=true"
            print(f"URL do Baserow: {url}")
            print(f"Headers: {headers}")
            
            try:
                start_time = time.time()
                # Usar o cliente HTTP compartilhado
                async with http_client.post(url, headers=headers, json=record_data) as response:
                    # Log da resposta bruta para debug
                    response_text = await response.text()
                    print(f"Resposta do Baserow: Status {response.status}, Conteúdo: {response_text}")
                    
                    # Verificar status da resposta
                    response.raise_for_status()
                    
                    # Converter a resposta para JSON
                    try:
                        result = await response.json()
                    except json.JSONDecodeError:
                        result = {"raw_response": response_text}
                    
                end_time = time.time()
                processing_time = round((end_time - start_time) * 1000, 2)
                print(f"Log registrado no Baserow para {phone_number} em {processing_time:.2f} ms")
                
                # Adicionar informações adicionais ao resultado
                if isinstance(result, dict):
                    result['processing_time_ms'] = processing_time
                    result['phone_number'] = phone_number
                    result['log_type'] = "system_message"
                    result['push_name_used'] = sender_name  # 🆕 Log do pushName usado
                
                return result
            
            except aiohttp.ClientResponseError as e:
                print(f"Erro HTTP ao registrar log no Baserow: {e}")
                if hasattr(e, 'response') and hasattr(e.response, 'text'):
                    response_text = await e.response.text()
                    print(f"Detalhes do erro: {response_text}")
                return {
                    "error": str(e),
                    "status_code": e.status if hasattr(e, 'status') else None,
                    "push_name_attempted": sender_name  # 🆕 Log do pushName que tentou usar
                }
            
            except Exception as e:
                print(f"Erro inesperado ao registrar log no Baserow: {e}")
                print(f"Traceback: {traceback.format_exc()}")
                return {
                    "error": str(e),
                    "status_code": 500,
                    "push_name_attempted": sender_name  # 🆕 Log do pushName que tentou usar
                }
    

        phone_number = state["phone_number"]
        thread_id = state["thread_id"]
        messages = state.get("messages", [])
        message_timestamps = state.get("message_timestamps", {})
        
        # Usar sempre o nome que vem do state (original do webhook)
        #push_name = state.get("name", "Sistema")


        
        print(f"\n[LOG-MESSAGES] 📝 Iniciando gravação SIMPLES")
        print(f"[LOG-MESSAGES] 📱 Phone: {phone_number}")
        print(f"[LOG-MESSAGES] 🧵 Thread: {thread_id}")
        #print(f"[LOG-MESSAGES] 👤 Push Name: '{push_name}'")
        print(f"[LOG-MESSAGES] 📨 Total de mensagens: {len(messages)}")
        
        # Gravar apenas a ÚLTIMA mensagem
        if not messages:
            print(f"[LOG-MESSAGES] ⏭️ Nenhuma mensagem para gravar")
            return state
        
        # Pegar apenas a última mensagem
        last_message = messages[-1]
        
        try:
            message_id = last_message.id
            message_content = last_message.content
            message_timestamp = message_timestamps.get(message_id, int(time.time() * 1000))
            
            # Usar o tipo real da mensagem (sem modificar)
            message_type = type(last_message).__name__
            
            if message_type == "AIMessage":
                push_name = os.getenv('AGENT_NAME', 'Sistema')
            else:
                push_name = state.get("name", "Sistema")

            event_type = state.get("event", None)

            print(f"[LOG-MESSAGES] 📝 Gravando ÚLTIMA mensagem")
            print(f"[LOG-MESSAGES]   ID: {message_id[:8]}...")
            print(f"[LOG-MESSAGES]   Tipo: {message_type}")
            print(f"[LOG-MESSAGES]   Push Name: '{push_name}'")
            print(f"[LOG-MESSAGES]   Conteúdo: {message_content[:50]}...")
            
            event_type = "send.message" if message_type == "AIMessage" else 'messages.upsert'

            # Gravar no Baserow com pushName original
            await upsert_message(
                phone_number=phone_number,
                message=message_content,
                message_type=message_type,
                event=event_type,
                http_client=http_client,
                push_name=push_name
            )
            
            print(f"[LOG-MESSAGES] ✅ Última mensagem gravada com pushName original")
            
        except Exception as e:
            print(f"[LOG-MESSAGES] ❌ Erro ao gravar última mensagem: {e}")
            print(f"[LOG-MESSAGES] ❌ Traceback: {traceback.format_exc()}")
        
        print(f"[LOG-MESSAGES] 🏁 Gravação simples concluída\n")
        
        return state

    def build_graph():
        builder = StateGraph(GeneralState)

        # Nós existentes
        builder.add_node("start_or_update", start_or_update)
        builder.add_node("check_combined_messages", check_combined_messages_node)
        builder.add_node("check_conversation_status", check_conversation_status)
        builder.add_node('classify_if_needed', classify_conversation)
        builder.add_node("log_messages", log_messages)
        builder.add_node("log_information", log_information)

        # 🔧 FLUXO SEQUENCIAL (sem paralelismo)
        builder.add_edge(START, "start_or_update")
        builder.add_edge("start_or_update", "check_combined_messages")
        builder.add_edge("check_combined_messages", "check_conversation_status")
        builder.add_edge("check_conversation_status", "classify_if_needed")
        builder.add_edge("classify_if_needed", "log_messages")       # log_messages primeiro
        builder.add_edge("log_messages", "log_information")          # depois log_information
        builder.add_edge("log_information", END)                     # e termina

        memory = InMemorySaver()
        graph = builder.compile(checkpointer=memory)
        return graph

    compiled = build_graph()

    return compiled

def create_support_graph_template(http_client: aiohttp.ClientSession, app_state: Any):
    """
    Template/Placeholder para criação de grafos de assistente médico
    
    Baseado na arquitetura original, este template fornece:
    - Estrutura completa de estados
    - Nós principais configuráveis
    - Sistema de sub-grafos
    - Controle HIL (Human In The Loop)
    - Sistema de classificação com regex + LLM
    - Observabilidade e logging
    
    Para usar: substitua os placeholders pelos seus casos específicos
    """

    # ==========================================
    # 1. DEFINIÇÃO DE ESTADOS
    # ==========================================
    
    class Assistant_State(TypedDict):
        # 📱 DADOS DO CLIENTE
        messages: Annotated[List[BaseMessage], add_messages]
        phone_number: str           
        name: str                   
        nome_assistente: str        
        specific_information: List  # Configurações específicas do negócio
        business_context: str       # Contexto do negócio
        previous_context: str       # Histórico do cliente
        
        # Campo para Output Estruturado
        structured_response: Optional[Dict[str, Any]]

        # ⚙️ CONTROLE DE ESTADO
        HIL: bool                   # Human In The Loop
        is_updating: bool           # Previne race conditions
        _should_ignore: Optional[bool]  # Context manager pode marcar para ignorar
        
        # 🎯 SISTEMA DE ATIVAÇÃO DE FLUXOS
        is_activation_flow: bool                          
        activation_flow_name: Optional[str]                 
        flow_to_intention_map: Optional[Dict[str, str]]   
        activation_context: Optional[Dict[str, Any]]      
        context_priming_active: bool                      
        last_activation_timestamp: Optional[int]          

        # 🧠 CLASSIFICAÇÃO E CONTEXTO
        tipo_conversa: Literal["NOVA", "CONTINUACAO", "nao_identificado"]
        confianca_primeira_classificacao_tipo: Literal["Alta", "Média", "Baixa"]
        confianca_primeira_classificacao_intencao: Literal["Alta", "Média", "Baixa"]
        raciocinio_primeira_classificacao: str

        # 🎯 INTENÇÕES (CUSTOMIZE AQUI)
        intencao: Optional[Literal[
            "PLACEHOLDER_INTENCAO_1",    # Ex: "Consulta para Cirurgias"
            "PLACEHOLDER_INTENCAO_2",    # Ex: "Consulta Problemas Respiratórios"
            "PLACEHOLDER_INTENCAO_3",    # Ex: "Outros"
        ]]
        
        # 📊 COMUNICAÇÃO ENTRE AGENTES
        _consultas_context: Optional[Dict[str, Any]]  # Context para sub-grafos
        _system_message_data: Optional[Dict[str, str]]  # Dados para SystemMessage

    # ==========================================
    # 2. MODELOS DE CLASSIFICAÇÃO
    # ==========================================
    
    class ClassificacaoContexto(BaseModel):
        tipo_conversa: Literal["NOVA", "CONTINUACAO", "nao_identificado"]
        intencao: Literal[
            "PLACEHOLDER_INTENCAO_1",    # Customize suas intenções aqui
            "PLACEHOLDER_INTENCAO_2", 
            "PLACEHOLDER_INTENCAO_3",
            "nao_identificado"
        ]
        confianca_tipo: Literal["Alta", "Média", "Baixa"]
        confianca_intencao: Literal["Alta", "Média", "Baixa"]
        raciocinio: str

    class AnaliseContexto(BaseModel):
        precisa_reclassificar: bool = False
        deve_ignorar: bool = False
        motivo: str
        contexto_adicional: List[str] = []

    # ==========================================
    # 3. CONFIGURAÇÃO DE LOGGING
    # ==========================================
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    def log_state_info(node_name: str, state: Assistant_State):
        """Função auxiliar para logging detalhado do estado"""
        logger.info(f"\n{'='*50}")
        logger.info(f"🔧 NÓ: {node_name}")
        logger.info(f"📱 Phone: {state.get('phone_number', 'N/A')}")
        logger.info(f"👤 Name: {state.get('name', 'N/A')}")
        logger.info(f"🏷️ HIL Status: {state.get('HIL', 'N/A')}")
        logger.info(f"🎯 Intenção: {state.get('intencao', 'N/A')}")
        
        messages = state.get('messages', [])
        logger.info(f"💬 Total de mensagens: {len(messages)}")
        
        if messages:
            for i, msg in enumerate(messages[-3:], 1):
                msg_type = "👤" if isinstance(msg, HumanMessage) else "🤖" if isinstance(msg, AIMessage) else "⚙️"
                content_preview = msg.content[:50] + "..." if len(msg.content) > 50 else msg.content
                logger.info(f"   {i}. {msg_type} {content_preview}")
        
        logger.info(f"{'='*50}\n")

    # ==========================================
    # 4. SISTEMA DE PRÉ-CLASSIFICAÇÃO COM REGEX
    # ==========================================
    
    def pre_classificar_com_regex(mensagem: str) -> Optional[Dict[str, str]]:
        """
        PLACEHOLDER: Adicione seus padrões regex específicos aqui
        
        Exemplo de uso:
        - Detectar padrões específicos do seu negócio
        - Classificação rápida sem usar LLM
        - Reduzir latência para casos comuns
        """
        import re
        
        texto_original = mensagem.strip()
        
        # ==========================================
        # EXEMPLO: PADRÃO PARA AGENDAMENTO
        # ==========================================
        # Padrão: (NÚMERO)Olá. Gostaria de agendar...
        padrao_agendamento = r'^\(\d{1,2}\)Olá\.\s*Gostaria\s+de\s+agendar'
        
        if re.search(padrao_agendamento, texto_original, re.IGNORECASE):
            return {
                'tipo_conversa': 'NOVA',
                'intencao': 'PLACEHOLDER_INTENCAO_1',  # Substitua pela sua intenção
                'confianca_tipo': 'Alta',
                'confianca_intencao': 'Alta',
                'raciocinio': 'Regex detectou padrão específico de agendamento'
            }
        
        # ==========================================
        # ADICIONE MAIS PADRÕES AQUI
        # ==========================================
        # TODO: Implementar padrões específicos do seu domínio
        
        return None  # Fallback para classificação LLM

    # ==========================================
    # 5. ANÁLISE DE CONTEXTO INTELIGENTE
    # ==========================================
    
    def should_reevaluate_context(state: Assistant_State) -> tuple[bool, str, List[str], bool]:
        """
        Analisa TODA a conversa para decisões contextuais inteligentes
        
        PLACEHOLDER: Customize a lógica de reavaliação para seu domínio
        """
        
        ultima_mensagem = state['messages'][-1]
        if not isinstance(ultima_mensagem, HumanMessage):
            return False, "Não é mensagem humana", [], False
        
        # Construir histórico completo
        conversa_completa = []
        for msg in state['messages']:
            if isinstance(msg, HumanMessage):
                conversa_completa.append(f"CLIENTE: {msg.content}")
            elif isinstance(msg, AIMessage):
                conversa_completa.append(f"SISTEMA: {msg.content}")
        
        historico = "\n".join(conversa_completa)
        intencao_atual = state.get('intencao', 'não definida')
        
        # PLACEHOLDER: Customize o prompt para seu domínio
        prompt_sistema = """
        Você é um analisador de contexto conversacional para [SEU DOMÍNIO].
        
        Analise TODO o histórico da conversa e determine:
        1. Se a última mensagem indica mudança real de intenção
        2. Se a mensagem deve ser IGNORADA (acumulada sem resposta)
        3. Extraia contexto adicional como lista de observações relevantes
        
        INTENÇÕES POSSÍVEIS:
        - PLACEHOLDER_INTENCAO_1: [Descrição]
        - PLACEHOLDER_INTENCAO_2: [Descrição]  
        - PLACEHOLDER_INTENCAO_3: [Descrição]
        
        CASOS PARA IGNORAR:
        - Mensagens muito vagas ("oi", "olá", "?")
        - Mensagens duplicadas/repetidas
        - Possíveis testes ou ruído
        - Confirmações simples que não agregam contexto
        """
        
        prompt_usuario = f"""
        HISTÓRICO COMPLETO DA CONVERSA:
        {historico}
        
        INTENÇÃO ATUAL CLASSIFICADA: {intencao_atual}
        
        Analise se a última mensagem do cliente indica mudança de intenção.
        """
        
        try:
            # TODO: Implementar chamada para seu LLM
            # completion = your_llm_client.parse(...)
            
            # PLACEHOLDER: Retorna exemplo
            return False, "Análise não implementada", [], False
            
        except Exception as e:
            print(f"[CONTEXT_MANAGER] ❌ Erro na análise LLM: {e}")
            return False, f"Erro na análise: {str(e)}", [], False

    # ==========================================
    # 6. CLASSIFICAÇÃO DE CONTEXTO
    # ==========================================
    
    def classificar_contexto_conversa(
        mensagem: str,
        context: Optional[str] = None,
        previous_context: Optional[str] = None
    ) -> 'ClassificacaoContexto':
        """
        PLACEHOLDER: Implemente sua lógica de classificação
        
        Classifica se uma mensagem é nova conversa ou continuação,
        e identifica a intenção do usuário.
        """
        
        try:
            # PLACEHOLDER: Customize o prompt para seu domínio
            prompt_sistema = f"""
            Você é um classificador de contexto conversacional para [SEU DOMÍNIO].

            Sua tarefa é analisar uma mensagem e determinar:
            1. Se é uma NOVA conversa ou CONTINUAÇÃO de conversa anterior
            2. Qual a intenção/assunto da mensagem

            TIPOS DE CONVERSA:
            - NOVA: Cliente iniciando nova solicitação ou assunto
            - CONTINUACAO: Resposta ou seguimento de conversa já em andamento
            - nao_identificado: Não consegue determinar com segurança

            INTENÇÕES POSSÍVEIS:
            - PLACEHOLDER_INTENCAO_1: [Sua descrição aqui]
            - PLACEHOLDER_INTENCAO_2: [Sua descrição aqui]
            - PLACEHOLDER_INTENCAO_3: [Sua descrição aqui]
            - nao_identificado: Não consegue identificar a intenção
            """

            prompt_usuario = f"""
            CONTEXTO DO NEGÓCIO: {context or "Não disponível"}
            HISTÓRICO DO CLIENTE: {previous_context or "Não disponível"}
            
            MENSAGEM DO USUÁRIO: {mensagem}
            
            Faça a classificação baseada nas informações disponíveis.
            """

            # TODO: Implementar chamada para seu LLM
            # completion = your_llm_client.parse(...)
            
            # PLACEHOLDER: Retorna classificação padrão
            return ClassificacaoContexto(
                tipo_conversa="NOVA",
                intencao="PLACEHOLDER_INTENCAO_3",
                confianca_tipo="Baixa",
                confianca_intencao="Baixa",
                raciocinio="Classificação placeholder - implemente sua lógica"
            )
            
        except Exception as e:
            logger.error(f"Erro ao classificar contexto: {e}")
            
            return ClassificacaoContexto(
                tipo_conversa="nao_identificado",
                intencao="nao_identificado",
                confianca_tipo="Baixa",
                confianca_intencao="Baixa",
                raciocinio=f"Erro na classificação: {str(e)}"
            )

    # ==========================================
    # 7. NÓS PRINCIPAIS DO GRAFO
    # ==========================================

    def hil_check_node(state: Assistant_State):
        """
        Primeiro nó: Filtra mensagens e verifica HIL
        Previne race conditions e verifica ownership
        """
        log_state_info("HIL_CHECK", state)
        
        # Habilitar flag de atualização
        try:
            state['is_updating'] = True
            logger.info("✅ Flag is_updating habilitada")
        except Exception as e:
            logger.error(f"❌ Erro ao habilitar flag: {e}")

        phone_number = state.get('phone_number')
    
        # 🕐 VERIFICAÇÃO TEMPORAL (previne race conditions)
        current_timestamp = int(time.time() * 1000)
        last_timestamp = app_state.last_message_timestamps.get(phone_number, 0)
        
        if current_timestamp <= last_timestamp:
            logger.info(f"⚠️ MENSAGEM FORA DE ORDEM - ignorando")
            return state
        
        app_state.last_message_timestamps[phone_number] = current_timestamp

        # 🤖 VERIFICAÇÃO DE OWNERSHIP (Human In The Loop)
        if hasattr(app_state, 'conversation_ownership') and phone_number in app_state.conversation_ownership:
            ownership = app_state.conversation_ownership[phone_number]
            
            if ownership.get('status') == 'human_active':
                logger.info(f"👩‍💼 ATENDENTE ATIVO para {phone_number} - parando processamento")
                return state

        # ✅ FILTRO: Verificar se última mensagem é humana
        if not state['messages']:
            logger.info("🚫 Nenhuma mensagem encontrada")
            return state
        
        ultima_mensagem = state['messages'][-1]
        
        if not isinstance(ultima_mensagem, HumanMessage):
            logger.info(f"🚫 Última mensagem é {type(ultima_mensagem).__name__} - não processando")
            return state
        
        logger.info("✅ Filtros passaram - continuando processamento")
        return state

    def context_manager_node(state: Assistant_State):
        """
        Nó para gestão de contexto - Analisa contexto e toma decisões
        """
        log_state_info("CONTEXT_MANAGER", state)
        
        try:
            # Análise completa do contexto
            needs_reclassify, reason, additional_context, should_ignore = should_reevaluate_context(state)
            
            print(f"[CONTEXT_MANAGER] 📋 Análise:")
            print(f"   🔄 Reclassificar: {needs_reclassify}")
            print(f"   🔇 Ignorar: {should_ignore}")
            print(f"   📝 Motivo: {reason}")
            
            # DECISÃO 1: Ignorar mensagem
            if should_ignore:
                state['_should_ignore'] = True
                state['_system_message_data'] = {
                    'phone_number': state.get('phone_number', ''),
                    'content': f"Context Manager: Mensagem ignorada - {reason}"
                }
                return state
            
            # DECISÃO 2: Re-classificação necessária
            if needs_reclassify:
                state['intencao'] = None
                state['raciocinio_primeira_classificacao'] = f"Re-análise: {reason}"
            
            # DECISÃO 3: Contexto adicional
            if additional_context:
                state['_consultas_context'] = {
                    'supervisor_intel': {
                        'context_manager_intel': {
                            'additional_context': additional_context,
                            'analysis_reason': reason,
                            'source': 'context_manager'
                        }
                    }
                }
            
            return state
            
        except Exception as e:
            print(f"[CONTEXT_MANAGER] ❌ ERRO: {str(e)}")
            return state

    def supervisor_node(state: Assistant_State):
        """
        Supervisor principal - Coordena fluxo e roteamento
        """
        log_state_info("SUPERVISOR", state)
        
        # 🔇 Verificar se deve ignorar mensagem
        if state.get('_should_ignore'):
            return state
        
        # 🎯 SISTEMA DE ATIVAÇÃO DE FLUXOS
        # TODO: Implementar lógica de ativação específica
        
        # 🔍 PRÉ-CLASSIFICAÇÃO COM REGEX
        ultima_mensagem = state['messages'][-1]
        if isinstance(ultima_mensagem, HumanMessage):
            mensagem_content = ultima_mensagem.content
            phone_number = state.get('phone_number', "")
            
            resultado_regex = pre_classificar_com_regex(mensagem_content)
            
            if resultado_regex:
                # ⚡ CLASSIFICAÇÃO RÁPIDA COM REGEX
                state['intencao'] = resultado_regex['intencao']
                state['tipo_conversa'] = resultado_regex['tipo_conversa']
                state['confianca_primeira_classificacao_tipo'] = resultado_regex['confianca_tipo']
                state['confianca_primeira_classificacao_intencao'] = resultado_regex['confianca_intencao']
                state['raciocinio_primeira_classificacao'] = resultado_regex['raciocinio']
                
                state['_system_message_data'] = {
                    'phone_number': phone_number,
                    'content': f"Classificação REGEX: {mensagem_content} → {resultado_regex['intencao']}"
                }
                
            else:
                # 🤖 FALLBACK PARA LLM
                state['intencao'] = "PLACEHOLDER_INTENCAO_3"  # Fallback padrão
                state['tipo_conversa'] = "NOVA"
                state['confianca_primeira_classificacao_tipo'] = "Baixa"
                state['confianca_primeira_classificacao_intencao'] = "Baixa"
                state['raciocinio_primeira_classificacao'] = "Fallback: Regex não encontrou padrão"
                
                state['_system_message_data'] = {
                    'phone_number': phone_number,
                    'content': f"Fallback LLM: {mensagem_content} → classificação padrão"
                }
        
        return state

    def classifier_node(state: Assistant_State):
        """
        Nó classificador com LLM
        """
        log_state_info("CLASSIFIER", state)
        
        if not state['messages']:
            return state
        
        ultima_mensagem = state['messages'][-1]
        if not isinstance(ultima_mensagem, HumanMessage):
            return state

        try:
            # Executar classificação
            resultado = classificar_contexto_conversa(
                ultima_mensagem.content, 
                state.get('business_context', ''), 
                state.get('previous_context', '')
            )
            
            # Atualizar estado
            state["tipo_conversa"] = resultado.tipo_conversa
            state["confianca_primeira_classificacao_tipo"] = resultado.confianca_tipo
            state['intencao'] = resultado.intencao
            state["confianca_primeira_classificacao_intencao"] = resultado.confianca_intencao
            state['raciocinio_primeira_classificacao'] = resultado.raciocinio
            
            # SystemMessage
            state['_system_message_data'] = {
                'phone_number': state.get('phone_number', ''),
                'content': f"LLM Classificação: {ultima_mensagem.content} → {resultado.intencao}"
            }
            
        except Exception as e:
            logger.error(f"❌ Erro na classificação: {e}")
            
            # Fallback
            state["tipo_conversa"] = "nao_identificado"
            state['intencao'] = "nao_identificado"
            state['raciocinio_primeira_classificacao'] = f"Erro: {str(e)}"
        
        return state

    # ==========================================
    # 8. SUB-GRAFOS PLACEHOLDER
    # ==========================================
    
    def placeholder_agent_1(state: Assistant_State):
        """
        PLACEHOLDER: Implemente seu primeiro agente específico
        
        Exemplo: Agente de agendamento, vendas, suporte, etc.
        """
        
        # 🆕 Inicializar structured_response
        state['structured_response'] = {
            'agent_id': 'placeholder_agent_1',
            'timestamp': int(time.time() * 1000),
            'execution_context': {
                'phone_number': state.get('phone_number'),
                'intencao': state.get('intencao'),
                'thread_id': state.get('thread_id', 'unknown')
            },
            'data': {},
            'status': 'initialized'
        }
        
        # Exemplo básico:
        state['HIL'] = True  # Transfere para humano
        
        # Adicionar resposta
        resposta = "Obrigado pelo contato! Nossa equipe vai te atender em breve."
        state['messages'].append(AIMessage(content=resposta))
        
        return state
    
    def placeholder_agent_2(state: Assistant_State):
        """
        PLACEHOLDER: Implemente seu segundo agente específico
        """
        log_state_info("AGENT_2_PLACEHOLDER", state)
        
        # TODO: Implementar lógica específica
        state['HIL'] = True
        
        resposta = "Entendi sua solicitação. Vou conectar você com nossa equipe especializada."
        state['messages'].append(AIMessage(content=resposta))
        
        return state
    
    def placeholder_agent_3(state: Assistant_State):
        """
        PLACEHOLDER: Implemente seu terceiro agente específico
        """
        log_state_info("AGENT_3_PLACEHOLDER", state)
        
        # TODO: Implementar lógica específica
        state['HIL'] = True
        
        #resposta = "Vou direcionar sua mensagem para nossa equipe. Aguarde um momento."
        #state['messages'].append(AIMessage(content=resposta))
        
        return state

    # ==========================================
    # 9. NÓS DE PROCESSAMENTO E LIMPEZA
    # ==========================================

    async def process_message_node(state: Assistant_State):
        """
        Processa e envia mensagens
        """
        log_state_info("PROCESS_MESSAGE", state)

        messages = state.get('messages', [])
        if not messages:
            return state

        ultima_mensagem = messages[-1]
        if isinstance(ultima_mensagem, AIMessage):
            print(f"📤 ENVIANDO: {ultima_mensagem.content}")
            # TODO: Implementar envio real
            # await send_message(state["phone_number"], ultima_mensagem.content, http_client)

        state['is_updating'] = False
        return state

    def cleanup_node(state: Assistant_State):
        """
        Nó de limpeza final
        """
        logger.info("🧹 Executando cleanup")
        
        try:
            state['is_updating'] = False
        except Exception as e:
            logger.error(f"❌ Erro no cleanup: {e}")
        
        return state

    # ==========================================
    # 10. FUNÇÕES DE DECISÃO
    # ==========================================

    def hil_decision(state: Assistant_State) -> Literal["Human_In_The_Loop", "Continue_Processing"]:
        """
        Decisão HIL: Verifica se deve continuar processamento automático
        """
        if not state['messages']:
            return "Human_In_The_Loop"
        
        ultima_mensagem = state['messages'][-1]
        if not isinstance(ultima_mensagem, HumanMessage):
            return "Human_In_The_Loop"
        
        if state.get('HIL', False):
            return "Human_In_The_Loop"
        
        return "Continue_Processing"

    def supervisor_decision(state: Assistant_State) -> Literal["needs_classification", "agent_1", "agent_2", "agent_3"]:
        """
        Decisão do supervisor: Rotea para classificação ou agentes
        """
        # Verificar se precisa classificar
        if not state.get('intencao'):
            return "needs_classification"
        
        # Rotear baseado na intenção
        intencao = state.get('intencao')
        
        # PLACEHOLDER: Customize o mapeamento
        intention_routing = {
            "PLACEHOLDER_INTENCAO_1": "agent_1",
            "PLACEHOLDER_INTENCAO_2": "agent_2", 
            "PLACEHOLDER_INTENCAO_3": "agent_3"
        }
        
        return intention_routing.get(intencao, "agent_3")

    def re_hil_decision(state: Assistant_State) -> Literal["Human_In_The_Loop", "Continue_Processing"]:
        """
        Re-verificação HIL antes do envio
        """
        phone_number = state.get('phone_number')
        
        # TODO: Verificar estado global da aplicação
        # if app_state.conversation_ownership.get(phone_number, {}).get('status') == 'human_active':
        #     return "Human_In_The_Loop"
        
        return "Continue_Processing"

    # ==========================================
    # 11. CONSTRUÇÃO DO GRAFO
    # ==========================================

    def build_graph():
        """
        Constrói o grafo principal com toda a arquitetura
        """
        builder = StateGraph(Assistant_State)
        
        # Adicionar nós
        builder.add_node("hil_check_node", hil_check_node)
        builder.add_node("context_manager_node", context_manager_node)
        builder.add_node("supervisor_node", supervisor_node)
        builder.add_node("classifier_node", classifier_node)
        builder.add_node("placeholder_agent_1", placeholder_agent_1)
        builder.add_node("placeholder_agent_2", placeholder_agent_2)
        builder.add_node("placeholder_agent_3", placeholder_agent_3)
        builder.add_node("process_message_node", process_message_node)
        builder.add_node("re_hil_check_node", lambda x: x)  # Placeholder
        builder.add_node("cleanup_node", cleanup_node)

        # Fluxo principal
        builder.add_edge(START, "hil_check_node")
        
        builder.add_conditional_edges("hil_check_node", hil_decision, {
            "Human_In_The_Loop": "cleanup_node",
            "Continue_Processing": "context_manager_node"
        })

        builder.add_edge("context_manager_node", "supervisor_node")

        builder.add_conditional_edges("supervisor_node", supervisor_decision, {
            'needs_classification': "classifier_node",
            'agent_1': "placeholder_agent_1",
            'agent_2': "placeholder_agent_2",
            'agent_3': "placeholder_agent_3"
        })
        
        builder.add_edge("classifier_node", "supervisor_node")

        # Fluxo pós-agentes
        builder.add_edge("placeholder_agent_1", "re_hil_check_node")
        builder.add_edge("placeholder_agent_2", "re_hil_check_node")
        builder.add_edge("placeholder_agent_3", "re_hil_check_node")

        builder.add_conditional_edges("re_hil_check_node", re_hil_decision, {
            "Human_In_The_Loop": "cleanup_node",
            "Continue_Processing": "process_message_node"
        })

        builder.add_edge('process_message_node', "cleanup_node")
        builder.add_edge('cleanup_node', END)
        
        # Compilar com memória
        memory = InMemorySaver()
        graph = builder.compile(checkpointer=memory)

        return graph

    compiled = build_graph()

    return compiled

def create_activation_graph(http_client: aiohttp.ClientSession, app_state: Any):

    class Activation_State(TypedDict):
        # Campos obrigatórios
        messages: Annotated[List[BaseMessage], add_messages]
        phone_number: str           
        name: str                   
        flow_name: str
        activation_context: List        
    
    def supervisor_node(state: Activation_State):
        
        print(f"[ACTIVATION_GRAPH] 🚀 Node Start - Flow: {state.get('flow_name')}")
        print(f"[ACTIVATION_GRAPH] 📱 Phone: {state.get('phone_number')}")
        print(f"[ACTIVATION_GRAPH] 📱 To -> : {state.get('name')}")
        print(f"[ACTIVATION_GRAPH] 📋 Context: {state.get('activation_context')}")
        
        return state
    
    def supervisor_decision(state: Activation_State) -> Literal["aniversarios", "placeholder", "ativacao_masterclass"]:
        
        # Rotear baseado na intenção
        flow_name = state.get('flow_name')
        
        # PLACEHOLDER: Customize o mapeamento
        intention_routing = {
            "aniversarios": "aniversarios",
            "indefinido": "placeholder", 
            "ativacao_masterclass": "ativacao_masterclass",
        }
        
        return intention_routing.get(flow_name, "placeholder")


    async def aniversario_flow_node(state: Activation_State):

        phone_number = state.get("phone_number")
        
        context = state.get("activation_context", [{}])
        nome = context[0].get("nome_pessoa", "amigo") if context else "amigo"
        tipo_fluxo = context[0].get("tipo_fluxo", "normal") if context else "normal"
        
        image_path = "/app/monitoring_dr_larissa/aniversarios_folder.jpg"

        # 🔧 CORREÇÃO: Mesmo dia do PRÓXIMO MÊS (não +30 dias)
        from datetime import datetime
        from dateutil.relativedelta import relativedelta
        
        hoje = datetime.now()
        # Adicionar exatamente 1 mês
        data_validade = (hoje + relativedelta(months=1)).strftime("%d/%m/%y")
        
        legenda_sabado_domingo = f"""Seu aniversário está chegando {nome} 🎉

Desejamos que o seu novo ciclo traga ainda mais motivos pra sorrir.

Para comemorar essa data especial, a Dra. Larissa Valadares preparou esse presente exclusivo para você, válido até {data_validade} (exceto consulta).

Conte sempre conosco! 💛"""

        legenda_dias_normais = f"""Parabéns {nome} 🎉

Desejamos que o seu novo ciclo traga ainda mais motivos pra sorrir.

Para comemorar essa data especial, a Dra. Larissa Valadares preparou esse presente exclusivo para você, válido até {data_validade} (exceto consulta).

Conte sempre conosco! 💛"""
 

        # ✅ LÓGICA CONDICIONAL
        if tipo_fluxo == "final_de_semana":
            legenda_escolhida = legenda_sabado_domingo
            print(f"[ANIVERSARIO] 📅 Usando mensagem de FINAL DE SEMANA para {nome}")
        else:
            legenda_escolhida = legenda_dias_normais
            print(f"[ANIVERSARIO] 📅 Usando mensagem de DIA NORMAL para {nome}")
        

        print(f"[ANIVERSARIO] 📅 Data de hoje: {hoje.strftime('%d/%m/%y')}")
        print(f"[ANIVERSARIO] 📅 Data validade (+1 mês): {data_validade}")
        print(f"[ANIVERSARIO] 🎯 Nome: {nome}")

        # ===== RESTO DO CÓDIGO IGUAL =====
        result = await send_base64_image_async(
            url_api_whatsapp=os.getenv("WHATSAPP_API_BASE_URL", "https://evolutionapi.gbstudios.tech"),
            instancia=os.getenv("WHATSAPP_API_INSTANCE"),
            api_key=os.getenv("WHATSAPP_API_TOKEN"),
            numero=phone_number,
            caminho_imagem=image_path,
            legenda=legenda_escolhida,
            http_client=http_client,
            app_state=app_state,
            is_activation_flow=True,
            flow_name=state.get("flow_name")
        )
        
        return state

    async def ativacao_masterclass_flow_node(state: Activation_State):

        phone_number = state.get("phone_number")
        
        context = state.get("activation_context", [{}])
        nome_contexto = context[0].get("nome", "").strip() if context else ""
        
        mensagem = f"""Buenas {nome_contexto}, como vai? 
Sou a Fabricia, mentora aqui do grupo Masterclass em Vendas. Que bom ter você conosco!

Talvez você não saiba, mas já estou na área de vendas faz um tempão e atualmente ajudo várias empresas e profissionais da saúde a crescerem seus negócios.

Estou aqui conversando com vocês para entender melhor o perfil de cada um, assim posso criar materiais que realmente agreguem valor na vida de vocês!

Me conta, qual é a sua área de atuação hoje?"""
        
        # ===== RESTO DO CÓDIGO IGUAL =====
        result = await send_ai_message(phone_number, mensagem, http_client)
        
        return state


    def placeholder_node(state: Activation_State):
        return state
        
    async def clean_up_node(state: Activation_State):
        
        print(f"[ACTIVATION_GRAPH] 🎯 Node End - Enviada mensagem de ativação")
        
       
        return state
    
    def build_graph():
    
        # Construção do agente
        builder_activation = StateGraph(Activation_State)
        
        # ✅ CORREÇÃO: Nós com nomes consistentes
        builder_activation.add_node("supervisor_node", supervisor_node)
        builder_activation.add_node("aniversario_flow_node", aniversario_flow_node)
        builder_activation.add_node("placeholder_node", placeholder_node)
        builder_activation.add_node("clean_up_node", clean_up_node)
        builder_activation.add_node("ativacao_masterclass_node", ativacao_masterclass_flow_node)
        
        builder_activation.add_edge(START, "supervisor_node")
        builder_activation.add_conditional_edges("supervisor_node", supervisor_decision,
        {'aniversarios': 'aniversario_flow_node',
        'placeholder': 'placeholder_node',
        'ativacao_masterclass': 'ativacao_masterclass_node'})
        
        builder_activation.add_edge("aniversario_flow_node", "clean_up_node")
        builder_activation.add_edge("ativacao_masterclass_node", "clean_up_node")
        builder_activation.add_edge("placeholder_node", "clean_up_node")

        builder_activation.add_edge("clean_up_node", END)
        
        
        activation_memory = InMemorySaver()
        graph = builder_activation.compile(checkpointer=activation_memory)

        return graph

    compiled = build_graph()

    return compiled

async def auto_queue_worker(app_state):
    """
    Worker que processa queue automaticamente a cada 5 segundos
    Usa a mesma lógica do endpoint manual
    """
    print("🤖 Auto-worker iniciado")
    
    cycle_count = 0  # Contador de ciclos para debug
    
    while app_state.auto_worker_running:
        try:
            cycle_count += 1
            current_time = int(time.time())
            queue_size = app_state.logging_queue.qsize()
            
            # 🆕 LOG DETALHADO A CADA CICLO
            #print(f"🤖 Auto-worker [Ciclo {cycle_count}] - {current_time}")
            #print(f"🤖   ✅ Flag running: {app_state.auto_worker_running}")
            #print(f"🤖   📦 Queue size: {queue_size}")
            #print(f"🤖   📊 Queue empty: {app_state.logging_queue.empty()}")
            
            # Verificar se há itens na fila
            if not app_state.logging_queue.empty():
                print(f"🤖 ✅ Auto-worker encontrou {queue_size} itens na fila")
                
                processed = 0
                errors = 0
                
                # Processar todos os itens (mesma lógica do endpoint)
                while not app_state.logging_queue.empty():
                    try:
                        #print(f"🤖   🔄 Processando item {processed + 1}...")
                        
                        # Pegar item da fila
                        item = app_state.logging_queue.get_nowait()
                        
                        # 🔍 ADICIONAR ESTE DEBUG AQUI:
                        print(f"🔍 DEBUG: Item state keys: {list(item['state'].keys())}")
                        print(f"🔍 DEBUG: Phone presente: {'phone_number' in item['state']}")
                        print(f"🔍 DEBUG: State completo: {item['state']}")
                        # 🆕 LOG DO ITEM
                        #print(f"🤖   📝 Item obtido: {type(item)}")
                        #print(f"🤖   🧵 Thread ID: {item.get('config', {}).get('configurable', {}).get('thread_id', 'N/A')}")
                        #print(f"🤖   🆕 Is new conversation: {item.get('is_new_conversation', 'N/A')}")
                        
                        # ✅ NOVA LÓGICA: verificar flag
                        if item["is_new_conversation"]:
                            #print(f"🤖   🆕 Nova thread - usando ainvoke direto")
                            logging_result = await app_state.logging_graph.ainvoke(
                                item["state"], 
                                item["config"]
                            )
                        else:
                            #print(f"🤖   🔄 Thread existente - usando update_state + ainvoke")
                            
                            # 🔍 DEBUG ANTES do update_state
                            #print(f"🔍 DEBUG: Estado ANTES do update_state:")
                            #print(f"🔍   📱 Phone: {item['state'].get('phone_number', 'N/A')}")
                            #print(f"🔍   👤 Name: {item['state'].get('name', 'N/A')}")
                            #print(f"🔍   💬 Messages count: {len(item['state'].get('messages', []))}")
                            #print(f"🔍   🧵 Thread ID: {item['config'].get('configurable', {}).get('thread_id', 'N/A')}")
                            #print(f"🔍   🆕 Is new conversation: {item.get('is_new_conversation', 'N/A')}")
                            
                            if item['state'].get('messages'):
                                last_msg = item['state']['messages'][-1]
                                #print(f"🔍   📝 Última mensagem tipo: {type(last_msg).__name__}")
                                #print(f"🔍   📝 Última mensagem conteúdo: {last_msg.content[:50]}...")
                            
                            ########## Campos para fazer o Update ###############
                            new_message = item["state"]["messages"][-1]
                            phone_number = item['state']["phone_number"]
                            thread_id = item['state']["thread_id"]
                            structured_response = item['state']['structured_response']
                            updated_config = app_state.logging_graph.update_state(
                                item["config"], 
                                {
                                    "messages": [new_message],
                                    "phone_number": phone_number,
                                    "thread_id": thread_id,
                                    "structured_response": structured_response
                                }
                            )
                            
                            # 🔍 DEBUG DEPOIS do update_state
                            #print(f"🔍 DEBUG: Configuração DEPOIS do update_state:")
                            #print(f"🔍   Config tipo: {type(updated_config)}")
                            #print(f"🔍   Config keys: {list(updated_config.keys()) if isinstance(updated_config, dict) else 'N/A'}")
                            
                            logging_result = await app_state.logging_graph.ainvoke(
                                {}, 
                                updated_config
                            )
                        
                        #print(f"🤖   ✅ Grafo executado com sucesso")
                        
                        processed += 1
                        app_state.logging_queue.task_done()
                        
                        #print(f"🤖   ✅ Item {processed} processado e marcado como done")
                        
                    except asyncio.QueueEmpty:
                        print(f"🤖   📭 Queue esvaziou durante processamento")
                        break
                    except Exception as e:
                        errors += 1
                        print(f"🤖   ❌ Auto-worker erro no item {processed + errors}: {e}")
                        print(f"🤖   ❌ Traceback: {traceback.format_exc()}")
                        try:
                            app_state.logging_queue.task_done()
                        except ValueError:
                            print(f"🤖   ⚠️ task_done() já foi chamado para este item")
                
                print(f"🤖 ✅ Auto-worker concluído: {processed} processados, {errors} erros")
                
                # 🆕 LOG PÓS-PROCESSAMENTO
                final_queue_size = app_state.logging_queue.qsize()
                print(f"🤖   📊 Queue size após processamento: {final_queue_size}")
                
            else:
                print(f"🤖 📭 Auto-worker: fila vazia, aguardando próximo ciclo...")
            
            # Aguardar 5 segundos antes da próxima verificação
            print(f"🤖 ⏰ Auto-worker: dormindo por 5 segundos...")
            await asyncio.sleep(5)
            
        except Exception as e:
            print(f"🤖 ❌ Erro crítico no auto-worker [Ciclo {cycle_count}]: {e}")
            print(f"🤖 ❌ Traceback crítico: {traceback.format_exc()}")
            print(f"🤖 🔄 Continuando execução após erro...")
            # Continua rodando mesmo com erro
            await asyncio.sleep(5)
    
    print(f"🤖 🛑 Auto-worker finalizado após {cycle_count} ciclos")
    print(f"🤖 🛑 Flag final: auto_worker_running={app_state.auto_worker_running}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    
    # Inicializa o cliente HTTP no app.state
    app.state.http_client = aiohttp.ClientSession(
    connector=aiohttp.TCPConnector(
        limit=30,                    # Pool total moderado
        limit_per_host=6,            # 3 apps × 2 conexões cada = suficiente
        ttl_dns_cache=300,           # 5min cache DNS
        force_close=False,           # Reutiliza conexões
        enable_cleanup_closed=True,  # 🔧 CRÍTICO para seu caso
        keepalive_timeout=60,        # 1min balance
    ),
    timeout=aiohttp.ClientTimeout(
        total=10,                    # Seu baseline: 130ms
        connect=5,                   
        sock_read=8                  
    ),
    # Parâmetros do ClientSession (não TCPConnector):
    auto_decompress=True,           # Compressão automática
    trust_env=True,                 # Usa vars de ambiente
)
    
    app.state.webhook_forward_endpoint = os.getenv('WEBHOOK_FORWARD_ENDPOINT')

    # Inicializa o estado na app.state
    app.state.threads = {}
    
    app.state.last_messages = {}
    
    app.state.combined_messages_status = {}
    
    app.state.background_tasks = {}

    app.state.conversation_ownership = {} # Para determinar se a conversa já está em HIL (Human in The Loop)

    app.state.last_message_timestamps = {}  # Para gravar timestamp da última mensagem enviada ou recebida
    
    app.state.thread = int(time.time()*1000)

    app.state.processing_locks = {}

    # Inicializar Apscheduler 
    try:
        scheduler = start_apscheduler()
        if scheduler:
            app.state.scheduler = scheduler
    except:
        print("Não foi possível iniciar o Apscheduler")

    # 🌌 INICIALIZAR CACHE PARA IDENTIFICAR GATILHOS DE DISPARO DE FLUXO VIA MENSAGENS DE ATIVAÇÃO
    if not hasattr(app.state, 'activation_cache'):
        app.state.activation_cache = {}
        print("✅ Cache de ativação inicializado")

    #####################################################################
    # O GRAFO DE LOGGING É COMPILADO AQUI 
    app.state.logging_graph = create_new_thread_graph_logging(http_client=app.state.http_client,
                                              app_state=app.state)
    #####################################################################

    ############################################################################
    # O GRAFO DE ATENDIMENTO É COMPILADO AQUI 
    app.state.support_graph = create_support_graph_template(
        http_client=app.state.http_client,
        app_state=app.state
    )
    ###########################################################################
    
    ###########################################################################
    # O GRAFO DE ATENDIMENTO É COMPILADO AQUI

    app.state.activation_graph = create_activation_graph(
        http_client=app.state.http_client,
        app_state=app.state
    )

    ###########################################################################



    ####### FILA É INICIADA AQUI ###########################################
    try:
        app.state.logging_queue = asyncio.Queue()
        print("Fila iniciada com sucesso")
    except:
        print("Problema ao iniciar a fila")
    ########################################################################
    
    # ✅ NOVO: Auto-worker setup
    app.state.auto_worker_running = True
    print("✅ Auto-worker flag ativada")
        
    # ✅ NOVO: Iniciar auto-worker
    app.state.auto_worker_task = asyncio.create_task(
        auto_queue_worker(app.state)
    )
    print("✅ Auto-worker iniciado (polling a cada 5s)")   
    ######################################
    
    

    yield

    # ✅ PROCESSAR ITENS RESTANTES NA FILA
    remaining_items = app.state.logging_queue.qsize()
    if remaining_items > 0:
        print(f"📦 Processando {remaining_items} itens restantes na fila...")
        
        processed = 0
        while not app.state.logging_queue.empty():
            try:
                item = app.state.logging_queue.get_nowait()
                await app.state.logging_graph.ainvoke(item["state"], item["config"])
                processed += 1
                print(f"✅ Item restante {processed} processado")
                
            except asyncio.QueueEmpty:
                break
            except Exception as e:
                print(f"❌ Erro ao processar item restante: {e}")
        
        print(f"✅ {processed} itens restantes processados")
    else:
        print("✅ Fila vazia - nada para processar")

    # # ✅ FECHAR CLIENTE HTTP
    if hasattr(app.state, "http_client") and not app.state.http_client.closed:
        await app.state.http_client.close()
    ####################################################################

    # Limpeza ao encerrar
    if hasattr(app.state, "http_client") and not app.state.http_client.closed:
        await app.state.http_client.close()
    print("Application shutting down")

@asynccontextmanager
async def processing_lock(phone_number: str, app_state):
    if phone_number not in app_state.processing_locks:
        app_state.processing_locks[phone_number] = asyncio.Lock()
    
    lock = app_state.processing_locks[phone_number]
    print(f"🔒 [LOCK] Aguardando lock para {phone_number}")
    
    async with lock:
        print(f"🔓 [LOCK] Lock obtido para {phone_number}")
        try:
            yield
        finally:
            print(f"🔒 [LOCK] Lock liberado para {phone_number}")

# Crie a aplicação com o lifespan
app = FastAPI(lifespan=lifespan, default_response_class=ORJSONResponse)

###### Rotas ###############
chat_router = APIRouter()

debug_router = APIRouter()

tasks_router = APIRouter()
#############################

async def send_text_message_async(instance_name: str,
                                  api_key: str,
                                  phone_number: str, 
                                  message: str, 
                                  http_client: aiohttp.ClientSession) -> Dict[str, Any]:
    """
    Versão assíncrona da função send_text_message
    """
    # Inicializa o dicionário de resultado
    result = {
        'success': False,
        'status_code': None,
        'message': '',
        'data': None
    }
    
    # Verificação de parâmetros
    if not all([instance_name, api_key, phone_number, message]):
        result['message'] = "Todos os parâmetros são obrigatórios: instance_name, api_key, phone_number, message"
        raise ValueError(result['message'])
    
    # URL do endpoint
    url = os.getenv("WHATSAPP_API_URL")
    
    # Cabeçalhos da requisição
    headers = {
        "Content-Type": "application/json",
        "apikey": api_key
    }
    
    # Dados para envio
    payload = {
        "number": phone_number,
        "text": message
    }
    

    try:
        start_time = time.time()
        # Usa o cliente HTTP passado como parâmetro
        async with http_client.post(url, headers=headers, json=payload, timeout=15) as response:
            # Atualiza o código de status no resultado
            result['status_code'] = response.status
            
            # Verifica se a requisição foi bem-sucedida (código 2xx)
            if 200 <= response.status < 300:
                result['success'] = True
                result['message'] = "Mensagem enviada com sucesso"
                
                try:
                    # Tenta converter o JSON para dicionário
                    result['data'] = await response.json()
                except:
                    result['success'] = False
                    result['message'] = "Resposta recebida, mas não é um JSON válido"
                    result['data'] = {'raw_response': await response.text()}
            else:
                # Se não foi bem-sucedida, tenta obter a mensagem de erro
                result['message'] = f"Erro na requisição: {response.status}"
                try:
                    result['data'] = await response.json()
                except:
                    result['data'] = {'raw_response': await response.text()}
        
        end_time = time.time()
        result['processing_time_ms'] = round((end_time - start_time) * 1000, 2)
        print(f"Tempo para envio assíncrono da mensagem: {result['processing_time_ms']:.2f} ms")
        
    except aiohttp.ClientError as e:
        result['message'] = f"Erro de conexão: {str(e)}"
        result['data'] = {'error_type': type(e).__name__}
    
    return result

async def insert_baserow_record_async(base_row_api_token: str, table_id: int, data: Dict[str, Any], base_url: str, http_client) -> Dict[str, Any]:
    """
    Versão assíncrona da função insert_baserow_record que utiliza um cliente HTTP compartilhado
    """
    # Verificação de parâmetros
    if not base_row_api_token:
        raise ValueError("O token de API é obrigatório")
    
    if not table_id:
        raise ValueError("O ID da tabela é obrigatório")
    
    if not data or not isinstance(data, dict):
        raise ValueError("Os dados devem ser fornecidos como um dicionário")
    
    # Configurar os cabeçalhos
    headers = {
        "Authorization": f"Token {base_row_api_token}",
        "Content-Type": "application/json"
    }
    
    # URL para criação do registro
    # Adicionar parâmetro user_field_names=true
    url = f"{base_url}/api/database/rows/table/{table_id}/?user_field_names=true"

    # Log detalhado antes da requisição
    print("\n==== TENTATIVA DE INSERÇÃO NO BASEROW ====")
    print(f"URL: {url}")
    print(f"Dados a inserir:")
    print(json.dumps(data, indent=2))

    # ADICIONE ESTES LOGS DETALHADOS AQUI
    print(f"COMPLETO - URL: {url}")
    print(f"COMPLETO - Dados: {json.dumps(data, indent=2)}")
    print(f"COMPLETO - Cabeçalhos: {headers}")
    
    try:
        start_time = time.time()
        # Usar o cliente HTTP compartilhado em vez de criar uma nova sessão
        async with http_client.post(url, headers=headers, json=data, timeout=15) as response:
            # Capturar o texto da resposta antes de levantar exceção
            response_text = await response.text()
            try:
                response_json = json.loads(response_text)
            except:
                response_json = {"raw_text": response_text}
                
            print(f"Status da resposta: {response.status}")
            print(f"Resposta do Baserow: {json.dumps(response_json, indent=2)}")
            
            # Agora levantar exceção se houver erro
            response.raise_for_status()
            
            # Se chegou aqui, não houve erro, converter a resposta para JSON
            result = response_json
            
        end_time = time.time()
        processing_time = round((end_time - start_time) * 1000, 2)
        print(f"Tempo para inserir registro assíncrono no Baserow: {processing_time:.2f} ms")
        
        # Adicionar tempo de processamento ao resultado
        if isinstance(result, dict):
            result['processing_time_ms'] = processing_time
        
        return result
    
    except aiohttp.ClientResponseError as e:
        print(f"Erro HTTP ao inserir registro: {e}")
        print(f"Detalhes completos do erro: {response_json}")
        # Retornar o erro em vez de levantar exceção para não interromper o processamento
        
            # Captura detalhes do erro
        try:
            error_details = await response.json()
        except:
            error_details = await response.text()
        
        print(f"Erro HTTP ao inserir registro: {e}")
        print(f"Detalhes completos do erro: {error_details}")
        
        print(f"Erro HTTP ao inserir registro: {e}")
    
        return {"error": str(e)}
    
    except aiohttp.ClientError as e:
        print(f"Erro de conexão ao inserir registro: {e}")
        return {"error": str(e)}
        
    except Exception as e:
        print(f"Erro inesperado ao inserir registro: {e}")
        return {"error": str(e)}

async def log_baserow_task(task):
    try:
        await task
    except Exception as e:
        print(f"Erro ao registrar no Baserow em segundo plano: {e}")

async def should_create_new_thread(phone_number: str, app_state):
   """
   Cria nova thread se não existe thread ou se houve inatividade > 8 horas
   """
   current_time = int(time.time() * 1000)
   inactivity_threshold = 8 * 60 * 60 * 1000  # 8 horas

   print(f"[THREAD-DEBUG] 📞 should_create_new_thread({phone_number})")
   print(f"[THREAD-DEBUG]   ⏰ Current time: {current_time}")
   print(f"[THREAD-DEBUG]   ⏱️ Threshold: {inactivity_threshold}ms ({inactivity_threshold//3600000}h)")

   # Se não tem thread registrada, criar nova
   if phone_number not in app_state.threads:
       print(f"[THREAD-DEBUG]   ✅ DECISÃO: CRIAR NOVA THREAD (sem thread anterior)")
       return True

   existing_thread_id = app_state.threads[phone_number]
   print(f"[THREAD-DEBUG]   ✅ Thread existente encontrada: {existing_thread_id}")

   # Inicializar dicionário de atividade se não existir
   if not hasattr(app_state, 'last_thread_activity'):
       app_state.last_thread_activity = {}
   
   # Se não tem registro de atividade, considerar como expirada
   if phone_number not in app_state.last_thread_activity:
       return True
   
   # Verificar inatividade
   last_activity = app_state.last_thread_activity[phone_number]
   inactive_time = current_time - last_activity
   
   if inactive_time > inactivity_threshold:
       return True  # Nova thread
   else:
       return False  # Manter thread existente

def determine_hil_status(
    is_outbound: bool, 
    is_from_human_agent: bool, 
    create_new_thread: bool, 
    current_hil_status: bool
) -> tuple[bool, str]:
    """
    Determina o status HIL baseado no contexto da mensagem.
    HIL = True é IRREVERSÍVEL. Uma vez ativo, sistema NUNCA mais responde automaticamente.
    VERSÃO v1: IA é puramente reativa (só responde, nunca inicia conversa)
    """
    
    # 🚨 REGRA CRÍTICA: HIL é irreversível
    if current_hil_status:
        return True, "HIL ativo - sistema PERMANENTEMENTE silencioso (irreversível)"
    
    if not is_outbound and create_new_thread:
        # Cenário 1: Cliente inicia nova conversa
        return False, "Cliente iniciou nova conversa - sistema processa automaticamente"
    
    elif not is_outbound and not create_new_thread:
        # Cenário 2: Cliente continua conversa automática  
        return False, "Conversa automática ativa - sistema continua processando"
    
    elif is_outbound and is_from_human_agent:
        # Cenário 3 + 4: Atendente intervém OU inicia conversa
        if create_new_thread:
            return True, "🚨 ATENDENTE INICIOU conversa - Thread NASCE em HIL permanente"
        else:
            return True, "🚨 ATENDENTE ASSUMIU controle - HIL permanente (irreversível)"
    
    elif is_outbound and not is_from_human_agent:
        if create_new_thread:
            return False, "IA iniciando conversa proativa - sistema ativo para responder"
        else:
            return False, "IA enviando em conversa existente - sistema continua ativo"
    
    else:
        return True, "Cenário não mapeado - modo seguro (silencioso permanente)"

async def insert_messages_into_graph(
   phone_number: str,
   message: str,
   name: str,
   app_state: Any,
   support_graph: Any,
   thread_id: str,
   is_new_conversation: bool,
   is_outbound: bool = False,
   is_from_human_agent: bool = False,
   message_type: str = "HumanMessage",
   is_activation_flow: bool = False,
   activation_flow_name: Optional[str] = None,
   flow_to_intention_map: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
   """
   🔧 TEMPLATE GENÉRICO: Inserção de mensagens em qualquer grafo
   
   Baseado na arquitetura medical assistant, mas adaptado para qualquer domínio.
   
   Args:
       phone_number: Identificador do cliente
       message: Conteúdo da mensagem
       name: Nome do cliente
       app_state: Estado global da aplicação
       support_graph: Grafo principal (nome genérico para qualquer domínio)
       thread_id: ID da thread/conversa
       is_new_conversation: Se é nova conversa
       is_outbound: Se mensagem é saída
       is_from_human_agent: Se vem de atendente humano
       message_type: Tipo da mensagem (Human/AI/System)
       is_activation_flow: Se é fluxo de ativação
       activation_flow_name: Nome do fluxo de ativação
       flow_to_intention_map: Mapeamento fluxo → intenção
   
   Returns:
       Dict com resultado da execução
       
   Note: 
       ASSISTANT_NAME, BUSINESS_CONTEXT e SPECIFIC_INFORMATION 
       devem ser configurados como constantes no template específico
   """
   
   async with processing_lock(phone_number, app_state):
       
       # 🛡️ FILTRO DE SEGURANÇA (OPCIONAL)
       # Descomente e customize conforme necessário:
       # authorized_numbers = getattr(app_state, 'authorized_numbers', None)
       # if authorized_numbers and phone_number not in authorized_numbers:
       #     print(f"[SECURITY] ❌ Número {phone_number} não autorizado")
       #     return {
       #         "status": "unauthorized",
       #         "phone_number": phone_number,
       #         "reason": "number_not_in_whitelist",
       #         "timestamp": int(time.time() * 1000)
       #     }
       
       print(f"[SECURITY] ✅ Número autorizado - processando...")

       # 📋 LOG DE ENTRADA
       print(f"\n[GRAPH-INSERT] 📥 Iniciando inserção de mensagem")
       print(f"[GRAPH-INSERT] 📱 Phone: {phone_number}")
       print(f"[GRAPH-INSERT] 💬 Message: {message[:50]}...")
       print(f"[GRAPH-INSERT] 👤 Name: {name}")
       print(f"[GRAPH-INSERT] 🆕 Message Type: {message_type}")
       print(f"[GRAPH-INSERT] 🔄 Is Outbound: {is_outbound}")
       print(f"[GRAPH-INSERT] 👩‍💼 Is From Human Agent: {is_from_human_agent}")
       print(f"[GRAPH-INSERT] 🧵 Thread ID: {thread_id}")
       print(f"[GRAPH-INSERT] 🆕 Nova conversa: {is_new_conversation}")
       print(f"[GRAPH-INSERT] 🎯 Activation Flow: {is_activation_flow}")
       
       # ===== CONFIGURAÇÃO DA THREAD =====
       thread_config = {"configurable": {"thread_id": thread_id}}
       print(f"[GRAPH-INSERT] 🔧 Thread config: {thread_config}")
       
       try:
           # ===== DETERMINAR STATUS HIL =====
           if is_new_conversation:
               hil_status, hil_reason = determine_hil_status(
                   is_outbound=is_outbound,
                   is_from_human_agent=is_from_human_agent,
                   create_new_thread=True,
                   current_hil_status=False
               )
           else:
               # Consultar HIL atual da thread existente
               current_state = support_graph.get_state(thread_config)
               print(f"🔍 DEBUG current_state type: {type(current_state)}")
               current_hil_status = current_state.values.get('HIL', False)

               if is_from_human_agent:
                   hil_status = True
                   hil_reason = "🚨 ATENDENTE assumiu controle"
               else:
                   hil_status = current_hil_status
                   hil_reason = "HIL mantido do estado anterior"
           
           print(f"[GRAPH-INSERT] 🏷️ HIL DECISÃO: {hil_status}")
           print(f"[GRAPH-INSERT] 📝 RAZÃO: {hil_reason}")
           
           # ===== CRIAR MENSAGEM BASEADA NO TIPO =====
           if message_type == "HumanMessage":
               graph_message = HumanMessage(content=message)
           elif message_type == "AIMessage":
               graph_message = AIMessage(content=message)
           elif message_type == "SystemMessage":
               graph_message = SystemMessage(content=message)
           else:
               # Fallback para HumanMessage
               graph_message = HumanMessage(content=message)
               print(f"[GRAPH-INSERT] ⚠️ Tipo desconhecido {message_type}, usando HumanMessage")
           
           # ===== PREPARAR INFORMAÇÕES ESPECÍFICAS DO DOMÍNIO =====
           # 🎯 PLACEHOLDER: Configure estas constantes no seu template específico
           ASSISTANT_NAME = "PLACEHOLDER_ASSISTANT_NAME"        # Ex: "Dra. Cátia", "Suporte Tech"
           BUSINESS_CONTEXT = "PLACEHOLDER_BUSINESS_CONTEXT"    # Ex: "Clínica médica..."
           SPECIFIC_INFORMATION = []  # Ex: [{"agenda": {...}}, {"produtos": [...]}]
           
           # ===== EXECUÇÃO DO GRAFO PRINCIPAL =====
           if is_new_conversation:
               # ===== CRIAÇÃO DE NOVA THREAD =====
               print(f"[GRAPH-INSERT] 🆕 NOVA THREAD - CRIAÇÃO INICIAL")
               
               # 🎯 ESTADO INICIAL GENÉRICO
               initial_state = {
                   "messages": [graph_message],
                   "phone_number": phone_number,
                   "name": name,
                   "nome_assistente": ASSISTANT_NAME,           # ← Constante do template
                   "specific_information": SPECIFIC_INFORMATION, # ← Constante do template
                   "HIL": hil_status,
                   "business_context": BUSINESS_CONTEXT,        # ← Constante do template
                   "is_activation_flow": is_activation_flow,
                   "activation_flow_name": activation_flow_name,
                   "flow_to_intention_map": flow_to_intention_map,
                   # 🆕 CAMPOS ADICIONAIS GENÉRICOS
                   "previous_context": "",  # Pode ser preenchido conforme necessário
                   "is_updating": False,
                   "_should_ignore": False,
                   "tipo_conversa": "nao_identificado",
                   "intencao": None,
                   "structured_response": None
                }
               
               print(f"[GRAPH-INSERT] 🆕 Payload de criação inicial:")
               print(f"[GRAPH-INSERT]   📱 phone_number: {phone_number}")
               print(f"[GRAPH-INSERT]   👤 name: {name}")
               print(f"[GRAPH-INSERT]   🏥 assistente: {ASSISTANT_NAME}")
               print(f"[GRAPH-INSERT]   🧵 thread_id: {thread_id}")
               print(f"[GRAPH-INSERT]   🏷️ HIL: {hil_status}")
               print(f"[GRAPH-INSERT]   🎯 activation_flow: {is_activation_flow}")
               
               print(f"[GRAPH-TIMING] ⏱️ ANTES ainvoke: {int(time.time() * 1000)}")
               
               # ✅ EXECUTAR GRAFO PRINCIPAL
               main_result = await support_graph.ainvoke(initial_state, thread_config)
               
               print(f"[GRAPH-TIMING] ⏱️ DEPOIS ainvoke: {int(time.time() * 1000)}")
               
           else:
               # ===== ATUALIZAÇÃO DE THREAD EXISTENTE =====
               print(f"[GRAPH-INSERT] 🔄 THREAD EXISTENTE - ATUALIZAÇÃO")
               
               # 🔍 DEBUG ANTES DO UPDATE
               print(f"🔍 DEBUG: ANTES do update_state")
               print(f"🔍   📱 Phone: {phone_number}")
               print(f"🔍   🧵 Thread ID: {thread_id}")
               print(f"🔍   💬 Mensagem tipo: {type(graph_message).__name__}")
               print(f"🔍   💬 Mensagem: {graph_message.content[:50]}...")

               # ⏳ AGUARDAR SE GRAFO ESTIVER OCUPADO
               print(f"🔄 Verificando se grafo está livre...")
               while True:
                   fresh_state = support_graph.get_state(thread_config)
                   is_updating = fresh_state.values.get('is_updating', False)
                   
                   if not is_updating:
                       print(f"✅ Grafo livre - pode atualizar")
                       break
                       
                   print(f"⏳ Grafo ocupado (is_updating=True) - aguardando...")
                   await asyncio.sleep(0.1)

               # ✅ UPDATE STATE
               updated_config = support_graph.update_state(thread_config, {
                   "messages": [graph_message],
                   "HIL": hil_status,
                   "is_activation_flow": is_activation_flow,
                   "activation_flow_name": activation_flow_name,
                   "flow_to_intention_map": flow_to_intention_map
               })

               post_update_state = support_graph.get_state(updated_config)
               messages_after_update = post_update_state.values.get('messages', [])
               print(f"🔍 APÓS UPDATE_STATE: {len(messages_after_update)} mensagens")
               for i, msg in enumerate(messages_after_update[-3:], 1):
                msg_type = "👤" if hasattr(msg, 'type') and msg.type == 'human' else "🤖"
                print(f"   {i}. {msg_type} {msg.content[:50]}...")   

            
               main_result = await support_graph.ainvoke({}, updated_config)

                # ✅ ADICIONAR ESTE LOG TAMBÉM:
               final_state = support_graph.get_state(updated_config)
               final_messages = final_state.values.get('messages', [])
               print(f"🔍 APÓS AINVOKE: {len(final_messages)} mensagens")


               #main_result = {"status": "updated", "config": updated_config}
           
           # ===== SISTEMA DE LOGGING ASSÍNCRONO =====
           logging_result = {"status": "error", "error": "not_processed"}
           
           try:
               # 📊 PREPARAR ESTADO PARA LOGGING
               logging_name = os.getenv('AGENT_NAME', 'Sistema') if (is_outbound and not is_from_human_agent) else name
               print(f"[DEBUG PushName no Grafo] - {logging_name}")
               
               if is_new_conversation:
                   logging_state = {
                       "messages": [graph_message],
                       "phone_number": phone_number,
                       "name": logging_name,
                       "start_time": int(time.time() * 1000),
                       "thread_id": thread_id,
                       "message_timestamps": {},
                       "need_classify": False,
                       # 🆕 CAMPOS GENÉRICOS PARA LOGGING
                       "assistant_name": ASSISTANT_NAME,
                       "business_context": BUSINESS_CONTEXT,
                       "domain": getattr(app_state, 'domain_name', 'generic'),
                       "structured_response": main_result.get('structured_response')
                   }
               else:
                   logging_state = {
                       "messages": [graph_message],
                       "phone_number": phone_number,
                       "name": logging_name,
                       "need_classify": False,
                       "thread_id": thread_id,
                       "assistant_name": ASSISTANT_NAME,
                       "domain": getattr(app_state, 'domain_name', 'generic'),
                       "structured_response": main_result.get('structured_response')
                   }
               
               # 🚀 ADICIONAR À FILA (NÃO BLOQUEIA)
               logging_item = {
                   "state": logging_state,
                   "config": thread_config,
                   "is_new_conversation": is_new_conversation,
                   "domain": getattr(app_state, 'domain_name', 'generic'),
                   "timestamp": int(time.time() * 1000)
               }
               
               print(f"[DEBUG-QUEUE] 🔍 ANTES de adicionar à fila:")
               print(f"[DEBUG-QUEUE]   📱 Phone: {logging_state.get('phone_number')}")
               print(f"[DEBUG-QUEUE]   👤 Name: {logging_state.get('name')}")
               print(f"[DEBUG-QUEUE]   🆕 Nova conversa: {is_new_conversation}")
               print(f"[DEBUG-QUEUE]   🏷️ Domínio: {logging_item.get('domain')}")

               # ✅ VERIFICAR SE FILA DE LOGGING EXISTE
               if hasattr(app_state, 'logging_queue'):
                   app_state.logging_queue.put_nowait(logging_item)
                   
                   print(f"[GRAPH-INSERT] ✅ Item adicionado à fila de logging")
                   print(f"[GRAPH-INSERT] 📊 Fila agora tem {app_state.logging_queue.qsize()} itens")
                   
                   logging_result = {
                       "status": "queued",
                       "queue_size": app_state.logging_queue.qsize(),
                       "message": "Adicionado à fila de logging"
                   }
               else:
                   print(f"[GRAPH-INSERT] ⚠️ Fila de logging não configurada")
                   logging_result = {
                       "status": "no_queue",
                       "message": "Sistema de logging não configurado"
                   }
               
           except asyncio.QueueFull:
               print(f"[GRAPH-INSERT] ⚠️ Fila cheia - item perdido")
               logging_result = {
                   "status": "queue_full",
                   "error": "Fila de logging cheia",
                   "queue_size": app_state.logging_queue.qsize()
               }
               
           except Exception as e:
               print(f"[GRAPH-INSERT] ❌ Erro ao adicionar à fila: {e}")
               logging_result = {
                   "status": "error",
                   "error": str(e),
                   "queue_size": getattr(app_state.logging_queue, 'qsize', lambda: 0)()
               }
           
           # ===== RESULTADO FINAL =====
           result = {
               "main_result": main_result,
               "logging_result": logging_result,
               "thread_id": thread_id,
               "is_outbound": is_outbound,
               "is_from_human_agent": is_from_human_agent,
               "hil_status": hil_status,
               "hil_reason": hil_reason,
               "phone_number": phone_number,
               "thread_created": is_new_conversation,
               "execution_type": "activation_flow_skipped" if is_activation_flow else "support_sync_logging_queued",
               "message_type": message_type,
               "timestamp": int(time.time() * 1000),
               # 🆕 CAMPOS GENÉRICOS ADICIONAIS
               "assistant_name": ASSISTANT_NAME,
               "domain": getattr(app_state, 'domain_name', 'generic')
           }
           
           print(f"[GRAPH-INSERT] 🎯 Inserção concluída")
           print(f"[GRAPH-INSERT] 📊 Grafo principal: {'pulado' if is_activation_flow else 'executado'}")
           print(f"[GRAPH-INSERT] 📦 Logging: {'na fila' if hasattr(app_state, 'logging_queue') else 'não configurado'}")
           
           return result
           
       except Exception as e:
           print(f"[GRAPH-INSERT] ❌ Erro crítico na inserção: {e}")
           print(f"[GRAPH-INSERT] ❌ Traceback: {traceback.format_exc()}")
           return {
               "error": str(e),
               "phone_number": phone_number,
               "thread_id": thread_id,
               "hil_status": hil_status if 'hil_status' in locals() else None,
               "message_type": message_type,
               "timestamp": int(time.time() * 1000),
               "domain": getattr(app_state, 'domain_name', 'generic')
           }

@chat_router.post("/send", response_model=None)
async def send_message(payload: dict, request: Request):
    try:
        start_time = time.time()

        # 🆕 ADICIONAR AQUI (após start_time):
        is_activation_flow = 'activation_flow' in payload
        flow_name = payload.get('flow_name', 'default') if is_activation_flow else 'normal'

        print(f"[/send] 🎯 Tipo de fluxo: {'ATIVAÇÃO' if is_activation_flow else 'NORMAL'}")
        if is_activation_flow:
            print(f"[/send] 📋 Flow Name: {flow_name}")
            
            # 🌌 CACHE PARA DOMINAÇÃO GALÁCTICA
            phone_number = payload.get('phone_number')  # ← PEGAR DO PAYLOAD
            cache_key = f"{phone_number}_{int(time.time())}"
            request.app.state.activation_cache[cache_key] = flow_name
            print(f"[/send] 🎯 Cache: {cache_key} = {flow_name}")
        
        # Obter o cliente HTTP compartilhado
        http_client = request.app.state.http_client
        print(f"Cliente HTTP obtido da app.state: {http_client is not None}")
        
        # Obter valores das variáveis de ambiente
        instance_name = os.getenv("WHATSAPP_API_INSTANCE")
        api_key = os.getenv("WHATSAPP_API_TOKEN")
        
        # Criar uma cópia do payload para não modificar o original
        payload_completo = payload.copy()
        
        # Adicionar os valores das variáveis de ambiente ao payload
        # Apenas se não foram fornecidos na requisição
        if "evolution_api_key" not in payload_completo:
            payload_completo["evolution_api_key"] = api_key
        if "instance_name" not in payload_completo:
            payload_completo["instance_name"] = instance_name
        
        # Validação manual mínima apenas do essencial
        if not all(k in payload_completo for k in ['phone_number', 'message', 'type']):
            print("ERRO: Parâmetros obrigatórios ausentes")
            return JSONResponse(status_code=400, content={"error": "Parâmetros obrigatórios ausentes (phone_number, message, type)"})
        
        # Determinar o ID da tabela a ser usado
        conversas_table_id = os.getenv("BASEROW_MESSAGE_TABLE_ID")
        grafos_table_id = os.getenv("BASEROW_GRAPH_TABLE_ID")
        instance_name = payload_completo.get('instance_name')
        
        #print(f"⚠️ Usando instância: {instance_name}, tabela de mensagens: {conversas_table_id} | tabela de grafos: {grafos_table_id}")

        # Envio da mensagem (agora passando o http_client)
        #print("\n--- Iniciando envio da mensagem via Evolution API ---")
        message_result = await send_text_message_async(
            payload_completo.get('instance_name'),
            payload_completo.get('evolution_api_key'),
            payload_completo.get('phone_number'),
            payload_completo.get('message'),
            http_client=http_client
        )
        print(f"Resultado do envio da mensagem: {message_result}")

        # ✅ CORREÇÃO APLICADA: Remover processamento do /send
        # O webhook receberá o evento 'send.message' e processará automaticamente
        print(f"[/send] ✅ Mensagem enviada - processamento será feito pelo webhook")
        print(f"[/send] 🎯 Webhook processará com pushName correto e sem duplicação")

        # Retorna o resultado padronizado
        end_time = time.time()
        processing_time = round((end_time - start_time) * 1000, 2)
        print(f"\nTempo total de processamento: {processing_time} ms")
        print("==== FIM DE PROCESSAMENTO DO ENDPOINT /send ====\n")
        
        return JSONResponse(content={
        "message_result": message_result,
        "baserow_logged": "delegated_to_webhook",
        "baserow_result": {"message": "Processamento delegado ao webhook"},
        "processing_delegated": True,
        "processing_time_ms": processing_time,
        "is_activation_flow": is_activation_flow,
        "flow_name": flow_name,
    })
        
    except Exception as e:
        print(f"ERRO CRÍTICO em /send: {e}")
        print(f"Detalhes do erro: {traceback.format_exc()}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@chat_router.post("/webhook", response_model=None)
async def process_whatsapp_webhook(request: Request):
    try:
        start_time = time.time()
        
        # Obtém o payload da requisição
        payload = await request.json()

        # SÓ para SystemMessage:
        #if payload.get('type') == 'SystemMessage':
        #    print(f"[WEBHOOK-SYSTEM-DEBUG] 📥 Payload recebido de SystemMessage:")
        #    print(f"[WEBHOOK-SYSTEM-DEBUG] Keys no payload: {list(payload.keys())}")
        #    print(f"[WEBHOOK-SYSTEM-DEBUG] Phone number extraído: {payload.get('data', {}).get('key', {}).get('remoteJid', 'NÃO_ENCONTRADO')}")

        #print(f"[/webhook] RECEBIDO EVENTO: {payload.get('event')}")

        # LOG ADICIONAL PARA DEBUG
        #print("[/webhook] Payload completo:", json.dumps(payload, indent=2))
        
        # Obter o cliente HTTP compartilhado
        http_client = request.app.state.http_client
        
        # Obtém o endpoint de encaminhamento do estado da aplicação
        forward_endpoint = request.app.state.webhook_forward_endpoint
        
        
        # Extrair os campos específicos necessários
        event = payload.get('event', '')
        instance = payload.get('instance', '')
        
        # Extrair dados aninhados
        data = payload.get('data', {})
        push_name = data.get('pushName', '')

        #print(f"[/webhook] pushName final: '{push_name}'")
        
        # Verificar se é uma mensagem do próprio sistema
        original_from_me = data.get('key', {}).get('fromMe', False)
        
        # Distinguir entre mensagens do bot e mensagens de atendentes humanos
        # ✅ NOVA LÓGICA (CORRETA):
        if event == 'send.message':
            # Sistema automático (/send)
            push_name = os.getenv('AGENT_NAME', 'Sistema')
            print(f"[DEBUG-PUSHNAME] 🔍 Evento: {event}")
            print(f"[DEBUG-PUSHNAME] 🔍 pushName original: '{push_name}'")
            print(f"[DEBUG-PUSHNAME] 🔍 fromMe: {original_from_me}")
            is_from_human_agent = False
            is_bot_message = True
        elif event == 'messages.upsert' and original_from_me:
            # Atendente humano
            is_from_human_agent = True
            is_bot_message = False
        else:
            # Cliente (messages.upsert + fromMe=false)
            is_from_human_agent = False
            is_bot_message = False

        # O resto do código permanece EXATAMENTE igual:
        from_me = original_from_me

        #print(f"[/webhook] FromMe original: {original_from_me}, Mensagem de atendente: {is_from_human_agent}, Mensagem do bot: {is_bot_message}, FromMe ajustado: {from_me}")
        
        # Extrair remoteJid (número do telefone)
        raw_remote_jid = data.get('key', {}).get('remoteJid', '').split('@')[0]
        
        # Normalizar o número de telefone
        def normalize_phone_number(phone: str):
            """
            Normaliza o número de telefone para um formato padrão:
            - Remove caracteres não numéricos
            - Verifica se o número começa com 55 (Brasil)
            - Garante que há o 9 na posição correta para celulares
            """
            # Remover todos os caracteres não numéricos
            digits_only = re.sub(r'\D', '', phone)
            
            # Se não começa com 55 (Brasil), adicionar
            if not digits_only.startswith('55'):
                digits_only = '55' + digits_only
            
            # Para celulares, garantir que o nono dígito (9) está presente
            # Se o número tiver DDD + 8 dígitos, inserir o 9
            if len(digits_only) == 12 and digits_only.startswith('55'):  # 55 + DDD (2) + número (8)
                ddd = digits_only[2:4]
                number = digits_only[4:]
                digits_only = '55' + ddd + '9' + number
            
            return digits_only
        
        # Aplicar a normalização
        remote_jid = normalize_phone_number(raw_remote_jid)
        
        # Adicionar logs para debug
        #print(f"[/webhook] Número original: {raw_remote_jid}, Normalizado: {remote_jid}")
        #print(f"[/webhook] FromMe original: {original_from_me}, Mensagem de atendente: {is_from_human_agent}, Mensagem do bot: {is_bot_message}, FromMe ajustado: {from_me}")
        
        
        # No seu webhook onde detectamos o áudio
        # Dentro da função process_whatsapp_webhook
        # Após extrair os dados da mensagem

        message_data = data.get('message', {})
        conversation = message_data.get('conversation', '')

        #Verificar se é uma mensagem com imagem
        if 'imageMessage' in message_data:
            caption = message_data['imageMessage'].get('caption', '')
            if caption:
                conversation = f"[Imagem recebida: {caption}]"
            else:
                conversation = "[Imagem recebida]"
            print(f"[IMAGEM] Processada: {conversation}")
        else:
            # Processar normalmente para outros tipos
            conversation = message_data.get('conversation', '')
        
        # Verificar se é uma mensagem de áudio
        if 'audioMessage' in message_data:
            # Extrair dados do áudio
            audio_data = message_data.get('audioMessage', {})
            
            # Extrair metadados importantes
            seconds = audio_data.get('seconds', 0)
            
            # Formatar a duração de forma amigável
            if seconds < 60:
                duration_text = f"{seconds} segundo{'s' if seconds != 1 else ''}"
            else:
                minutes = seconds // 60
                remaining_seconds = seconds % 60
                if remaining_seconds == 0:
                    duration_text = f"{minutes} minuto{'s' if minutes > 1 else ''}"
                else:
                    duration_text = f"{minutes} minuto{'s' if minutes > 1 else ''} e {remaining_seconds} segundo{'s' if remaining_seconds != 1 else ''}"
            
            # Criar mensagem amigável
            conversation = f"[Áudio de {duration_text}]"
            
            # Gerar ID único para referência futura se necessário
            audio_id = str(uuid.uuid4())
            
            print(f"[ÁUDIO] Detectado áudio de {duration_text}. ID: {audio_id}")
        
                # 📹 Vídeos
        
        if 'videoMessage' in message_data:
            video_data = message_data.get('videoMessage', {})
            caption = video_data.get('caption', '')
            seconds = video_data.get('seconds', 0)
            
            if caption:
                conversation = f"[Vídeo de {seconds}s recebido: {caption}]"
            else:
                conversation = f"[Vídeo de {seconds}s recebido]"

        # 📄 Documentos
        if 'documentMessage' in message_data:
            doc_data = message_data.get('documentMessage', {})
            filename = doc_data.get('fileName', 'documento')
            
            conversation = f"[Documento recebido: {filename}]"

        # 📍 Localização
        if 'locationMessage' in message_data:
            conversation = "[Localização compartilhada]"


        # Extrair timestamp da mensagem
        message_timestamp = int(time.time()*1000)

        # ✅ CORREÇÃO: Determinar message_type - VERIFICAR PAYLOAD PRIMEIRO
        payload_type = payload.get('type')  # SystemMessage vem aqui!
        
        if payload_type:
            # Se veio tipo específico no payload (ex: SystemMessage)
            message_type = payload_type
            print(f"[/webhook] Tipo de mensagem do payload: {message_type}")
        elif event == 'send.message':
            # Fallback para eventos send.message sem tipo específico
            message_type = "AIMessage"
            print(f"[/webhook] Tipo de mensagem por evento: {message_type} (send.message)")
        else:  # messages.upsert
            # Mensagens recebidas de usuários
            message_type = "HumanMessage"
            print(f"[/webhook] Tipo de mensagem por evento: {message_type} (messages.upsert)")

        # ✅ MANTER: Verificação adicional em message_data (para casos específicos)
        if message_data and isinstance(message_data, dict) and "type" in message_data:
            message_data_type = message_data.get("type")
            print(f"[/webhook] Tipo também encontrado em message_data: {message_data_type}")
            # Só sobrescrever se não tínhamos tipo no payload
            if not payload_type:
                message_type = message_data_type

        print(f"[/webhook] Tipo de mensagem FINAL determinado: {message_type}")

        # Verificar se há um endpoint de encaminhamento configurado
        forwarding_result = None
        if forward_endpoint and conversation:
            try:
                # Configurar a requisição para o endpoint de destino com número normalizado
                forward_payload = {
                    "conversation": conversation,
                    "phone_number": remote_jid,  # Usando o número normalizado
                    "push_name": push_name,
                    "from_me": from_me,          # TRUE apenas para mensagens de atendentes humanos
                    "is_bot_message": is_bot_message,  # TRUE para mensagens automáticas do bot
                    "original_from_me": original_from_me,
                    "raw_phoneNumber": raw_remote_jid,
                    "message_type": message_type,
                    "message_timestamp": message_timestamp,
                      # O valor original para referência
                }
                
                #print(f"[/webhook] Encaminhando para {forward_endpoint} com número {remote_jid}")
                
                async with http_client.post(forward_endpoint, json=forward_payload, timeout=10) as forward_response:
                    forwarding_result = {
                        "status_code": forward_response.status,
                        "response": await forward_response.json() if forward_response.content_type == 'application/json' else None
                    }
                    
            except Exception as forward_error:
                print(f"[/webhook] Erro ao encaminhar: {str(forward_error)}")
                forwarding_result = {
                    "error": str(forward_error)
                }
        
        # Inicializar variáveis para o resultado da inserção no Baserow
        baserow_logged = False
        baserow_result = None
        
        # Retorna o resultado padronizado - IMPORTANTE: este return está FORA do bloco except do Baserow
        end_time = time.time()
        return JSONResponse(content={
            "baserow_logged": baserow_logged,
            "baserow_result": baserow_result,
            "forwarded": bool(forward_endpoint),
            "forwarding_result": forwarding_result,
            "original_phone": raw_remote_jid,
            "normalized_phone": remote_jid,
            "is_bot_message": is_bot_message,
            "is_from_human_agent": is_from_human_agent,
            "from_me_original": original_from_me,
            "from_me_adjusted": from_me,
            "processing_time_ms": round((end_time - start_time) * 1000, 2)
        })
    
    except json.JSONDecodeError:
        return JSONResponse(status_code=400, content={"error": "Payload inválido - JSON mal formatado"})
    
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "error": str(e),
            "details": traceback.format_exc()
        })

@chat_router.post("/agent", response_model=None)
async def receive_return_messages(request: Request):
    
    # Tempo máximo de intervalo para combinar mensagens (5 segundos)
    message_combine_threshold = 5 * 1000

    try:
        payload = await request.json()
        #print("[/agent] Payload completo:", json.dumps(payload, indent=2))
        
        http_client = request.app.state.http_client

        conversation = payload.get('conversation')
        phone_number = payload.get('phone_number', '')
        raw_phone_number = payload.get("raw_phoneNumber")
        
        pushName = (os.getenv('AGENT_NAME', "guardIA agente IA") 
                   if payload.get('is_bot_message', False) 
                   else payload.get('push_name', "Cliente"))
        
        # ✅ ADICIONE ESTES LOGS DE DEBUG:
        print(f"[DEBUG-PUSHNAME-AGENT] 🔍 is_bot_message: {payload.get('is_bot_message', False)}")
        print(f"[DEBUG-PUSHNAME-AGENT] 🔍 pushName original: {payload.get('push_name', 'VAZIO')}")
        print(f"[DEBUG-PUSHNAME-AGENT] 🔍 pushName FINAL: {pushName}")
        print(f"[DEBUG-PUSHNAME-AGENT] 🔍 AGENT_NAME env: {os.getenv('AGENT_NAME', 'NAO_DEFINIDO')}")

        message_timestamp = payload.get('message_timestamp')

        # Gravar timestamp da última mensagem
        if message_timestamp:
            # Salvar no app.state
            request.app.state.last_message_timestamps[phone_number] = message_timestamp
            print(f"🕐 Timestamp salvo: {phone_number} → {message_timestamp}")

        # 🌌 VERIFICAR CACHE FLUXO DE ATIVAÇÃO
        is_activation_flow = False
        flow_name = "normal"

        for key in list(request.app.state.activation_cache.keys()):
            if key.startswith(f"{phone_number}_"):
                flow_name = request.app.state.activation_cache.pop(key)
                is_activation_flow = True
                print(f"[/agent] 🎯 Detectado: {flow_name}")
                break
        
        # 🆕 OBTER O message_type DO WEBHOOK
        webhook_message_type = payload.get('message_type', 'HumanMessage')
        
        # 🆕 IDENTIFICAÇÃO CORRETA DE ORIGEM DAS MENSAGENS
        from_me = payload.get('from_me', False)  # TRUE apenas para mensagens de atendentes humanos
        is_bot_message = payload.get('is_bot_message', False)  # TRUE para mensagens automáticas do bot
        original_from_me = payload.get('original_from_me', False)  # Valor original de fromMe

        # 🆕 LÓGICA CLARA DE CLASSIFICAÇÃO
        is_human_agent_message = from_me and not is_bot_message
        is_system_message = from_me and is_bot_message
        is_client_message = not from_me

        #print(f"[/agent] 📝 Análise da mensagem:")
        #print(f"[/agent] 📱 Phone: {phone_number}")
        #print(f"[/agent] 👤 Push Name: {pushName}")
        print(f"[/agent] 💬 Message: {conversation[:50]}...")
        #print(f"[/agent] 🆕 Message Type (webhook): {webhook_message_type}")  # 🆕 LOG NOVO
        #print(f"[/agent] 🏷️ from_me: {from_me}")
        #print(f"[/agent] 🤖 is_bot_message: {is_bot_message}")
        #print(f"[/agent] 👩‍💼 Is Human Agent: {is_human_agent_message}")
        #print(f"[/agent] 🔧 Is System Message: {is_system_message}")
        #print(f"[/agent] 👤 Is Client Message: {is_client_message}")

        # Verificar inicialização de estado
        if not hasattr(request.app.state, 'threads'):
            request.app.state.threads = {}
        if not hasattr(request.app.state, 'last_messages'):
            request.app.state.last_messages = {}
        if not hasattr(request.app.state, 'combined_messages_status'):
            request.app.state.combined_messages_status = {}
        if not hasattr(request.app.state, 'conversation_ownership'):
            request.app.state.conversation_ownership = {}
        # 🕐 TIMESTAMP TRACKING (se ainda não existe)
        if not hasattr(app.state, 'last_message_timestamps'):
            request.app.state.last_message_timestamps = {}

        # ===== LÓGICA DE COMBINAÇÃO DE MENSAGENS (MANTIDA IGUAL) =====
        current_time = int(time.time() * 1000)
        combined_message = conversation
        should_combine = False
        message_to_process = conversation
        
        # 🆕 SÓ COMBINAR MENSAGENS DE CLIENTES (não de atendentes ou sistema)
        if is_client_message:
            # Limpar status anterior
            if phone_number in request.app.state.combined_messages_status:
                #print(f"[DEBUG-COMBINE] Limpando status anterior para {phone_number}")
                del request.app.state.combined_messages_status[phone_number]
            
            if phone_number in request.app.state.last_messages:
                last_message = request.app.state.last_messages[phone_number]
                time_diff = current_time - last_message["timestamp"]
                
                #print(f"[DEBUG-COMBINE] Última mensagem: '{last_message['text']}'")
                #print(f"[DEBUG-COMBINE] Intervalo: {time_diff} ms")
                
                # Combinar mensagens se intervalo <= threshold
                if time_diff <= message_combine_threshold:
                    combined_message = f"{last_message['text']} {conversation}"
                    should_combine = True
                    #print(f"[DEBUG-COMBINE] Combinando: '{combined_message}'")

                    request.app.state.combined_messages_status[phone_number] = {
                        "has_combined_messages": True,
                        "combined_message": combined_message,
                        "timestamp": current_time,
                        "time_diff_ms": time_diff
                    }

                    message_to_process = combined_message
                    #print(f"[DEBUG-COMBINE] Flag definida: has_combined_messages=True")
            
            # Atualizar registro da última mensagem (só para clientes)
            request.app.state.last_messages[phone_number] = {
                "text": conversation,  # Mensagem original, não combinada
                "timestamp": current_time
            }
        else:
            print(f"[DEBUG-COMBINE] Mensagem de atendente/sistema - não combinando")

        # ===== PROCESSAMENTO UNIFICADO (NOVA ABORDAGEM) =====
        
        # Determinar message_to_process final
        if is_client_message:
            final_message = message_to_process  # Já definido na lógica de combinação
        else:
            final_message = conversation  # Atendente/Sistema usam mensagem original

        # Determinar parâmetros da função
        if is_human_agent_message:
            print("[/agent] #### MENSAGEM DE ATENDENTE HUMANO #####")
            process_is_outbound = True
            process_is_from_human_agent = True

            request.app.state.conversation_ownership[phone_number] = {
                'status': 'human_active',
                'timestamp': int(time.time() * 1000),
                'agent_detected': True
            } #Gravando no estado que está em atendimento humano

            
        elif is_system_message:
            print("[/agent] #### MENSAGEM DO SISTEMA (BOT) #####")
            process_is_outbound = True
            process_is_from_human_agent = False
            
        else:  # is_client_message
            print("[/agent] #### MENSAGEM DE CLIENTE REGULAR #####")
            process_is_outbound = False
            process_is_from_human_agent = False


         #Verificar se threads foi inicializado
        if not hasattr(request.app.state, 'threads'):
            request.app.state.threads = {}
            #print("🔧 Inicializando dicionário de threads")

        # ✅ USAR A FUNÇÃO should_create_new_thread QUE JÁ EXISTE
        should_create_new = await should_create_new_thread(
            phone_number, 
            request.app.state)
        
        print(f"[THREAD-DEBUG] Should create new: {should_create_new}")

        if should_create_new or phone_number not in request.app.state.threads:
            # Criar nova thread
            thread_id = generate_uuid()
            request.app.state.threads[phone_number] = thread_id
            
            # 🔧 CORREÇÃO: Atualizar atividade imediatamente
            if not hasattr(request.app.state, 'last_thread_activity'):
                request.app.state.last_thread_activity = {}
            request.app.state.last_thread_activity[phone_number] = int(time.time() * 1000)
            
            print(f"🆕 Nova thread criada para {phone_number}: {thread_id}")
            is_new_conversation = True
        else:
            # Usar thread existente
            thread_id = request.app.state.threads[phone_number]
            print(f"♻️ Usando thread existente para {phone_number}: {thread_id}")
            is_new_conversation = False


        is_activation_flow_value=is_activation_flow

        # ===== UMA ÚNICA CHAMADA UNIFICADA =====
        result = await insert_messages_into_graph(
            phone_number=phone_number,
            message=final_message,
            name=pushName,
            app_state=request.app.state,
            support_graph=request.app.state.support_graph,
            thread_id=thread_id,  # ✅ OBRIGATÓRIO
            is_new_conversation=is_new_conversation,  # ✅ OBRIGATÓRIO
            is_outbound=process_is_outbound,  # ✅ OPCIONAL
            is_from_human_agent=process_is_from_human_agent,  # ✅ OPCIONAL
            message_type=webhook_message_type,
            is_activation_flow=is_activation_flow_value,
            activation_flow_name=flow_name,
            flow_to_intention_map=FLOW_TO_INTENTION_MAP,  # ✅ OPCIONAL
            # ✅ ADICIONAR: Campos do Guga com valores padrão
        )

        # Atualizar atividade da thread
        if not hasattr(request.app.state, 'last_thread_activity'):
            request.app.state.last_thread_activity = {}
        request.app.state.last_thread_activity[phone_number] = int(time.time() * 1000)

        # Log de confirmação
        message_types = ["Cliente", "Sistema", "Atendente"]
        type_index = int(process_is_outbound) + int(process_is_from_human_agent)
        print(f"[/agent] ✅ {message_types[type_index]} processado")

        # ===== PREPARAR RESPOSTA (MANTIDA IGUAL) =====
        
        has_combined_messages = False
        combined_status = None
        if phone_number in request.app.state.combined_messages_status:
            has_combined_messages = request.app.state.combined_messages_status[phone_number].get("has_combined_messages", False)
            combined_status = request.app.state.combined_messages_status[phone_number]

        return {
            "status": "success", 
            "conversation": conversation, 
            "phone_number": phone_number, 
            "result": result,
            "has_combined_messages": has_combined_messages,
            "combined_status": combined_status,
            "combined_message": combined_message if should_combine else None,
            "is_human_agent": is_human_agent_message,
            "is_system_message": is_system_message,
            "is_client_message": is_client_message,
            "message_processed": message_to_process,
            # 🆕 Informações HIL para debug
            "hil_info": {
                "hil_status": result.get("hil_status") if isinstance(result, dict) else None,
                "hil_reason": result.get("hil_reason") if isinstance(result, dict) else None
            }
        }
    
    except Exception as e:
        print(f"[/agent] ❌ Erro ao processar: {str(e)}")
        print(f"[/agent] ❌ Traceback: {traceback.format_exc()}")
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }
    finally:
        print("[/agent] 🏁 Processamento finalizado")

@chat_router.post("/test-system-message", response_model=None)
async def test_system_message(request: Request):
    """
    Endpoint de teste para validar se SystemMessages entram corretamente no grafo
    
    Body esperado:
    {
        "phone_number": "5511999999999",
        "message": "Teste de SystemMessage - usuário conectado ao sistema"
    }
    """
    try:
        start_time = time.time()
        
        # Obter payload da requisição
        payload = await request.json()
        
        # Validar parâmetros obrigatórios
        phone_number = payload.get('phone_number')
        message = payload.get('message')
        
        if not phone_number or not message:
            return JSONResponse(
                status_code=400, 
                content={
                    "error": "Parâmetros obrigatórios: phone_number, message",
                    "example": {
                        "phone_number": "5511999999999",
                        "message": "Teste de SystemMessage"
                    }
                }
            )
        
        # Obter cliente HTTP compartilhado
        http_client = request.app.state.http_client
        
        print(f"[TEST-SYSTEM] 🧪 Iniciando teste de SystemMessage")
        print(f"[TEST-SYSTEM] 📱 Phone: {phone_number}")
        print(f"[TEST-SYSTEM] 💬 Message: {message}")
        
        # Chamar a função send_system_message
        result = await send_system_message(
            phone_number=phone_number,
            message=message,
            http_client=http_client
        )
        
        # Verificar se houve erro
        if "error" in result:
            print(f"[TEST-SYSTEM] ❌ Erro na send_system_message: {result['error']}")
            return JSONResponse(
                status_code=500,
                content={
                    "test_status": "FAILED",
                    "error": result["error"],
                    "step_failed": "send_system_message"
                }
            )
        
        # Aguardar um pouco para o processamento assíncrono
        print(f"[TEST-SYSTEM] ⏳ Aguardando processamento assíncrono...")
        await asyncio.sleep(2)  # 2 segundos para garantir que processou
        
        # Verificar se a mensagem entrou no grafo
        # (Aqui você pode adicionar uma consulta ao grafo se tiver uma função para isso)
        
        end_time = time.time()
        processing_time = round((end_time - start_time) * 1000, 2)
        
        print(f"[TEST-SYSTEM] ✅ Teste concluído em {processing_time}ms")
        
        return JSONResponse(content={
            "test_status": "SUCCESS",
            "message": "SystemMessage enviada com sucesso! 🎯",
            "phone_number": phone_number,
            "test_message": message,
            "send_system_message_result": result,
            "processing_time_ms": processing_time,
            "logs_to_watch": [
                "🔍 PROCURE ESTES LOGS:",
                "[send_system_message] 📤 Enviando para webhook",
                "[/webhook] RECEBIDO EVENTO: send.message", 
                "[/webhook] Tipo de mensagem FINAL determinado: SystemMessage",
                "[/agent] #### MENSAGEM DO SISTEMA (BOT) #####",
                "[/agent] ✅ Sistema processado"
            ],
            "success_criteria": "✅ Se todos os logs acima aparecerem, o teste foi bem-sucedido!"
        })
        
    except Exception as e:
        print(f"[TEST-SYSTEM] 💥 Erro crítico: {str(e)}")
        print(f"[TEST-SYSTEM] 📋 Traceback: {traceback.format_exc()}")
        
        return JSONResponse(
            status_code=500,
            content={
                "test_status": "FAILED", 
                "error": str(e),
                "traceback": traceback.format_exc(),
                "step_failed": "endpoint_execution"
            }
        )

@chat_router.post("/graph-state")
async def get_graph_state(payload: dict, request: Request):
    # Validar se thread_id está presente
    if "thread_id" not in payload:
        return {
            "error": "thread_id é obrigatório",
            "exemplo": {
                "thread_id": "uuid-da-thread",
                "graph": "support"
            }
        }
    
    thread_id = payload["thread_id"]
    graph = payload.get("graph", "support")
    
    # Validar se thread_id não está vazio
    if not thread_id or str(thread_id).strip() == "":
        return {
            "error": "thread_id não pode estar vazio",
            "thread_id_recebido": thread_id
        }
    
    config = {"configurable": {"thread_id": thread_id}}
    
    try:
        if graph == "support":
            return request.app.state.support_graph.get_state(config).values
        elif graph == "logging":
            return request.app.state.logging_graph.get_state(config).values
        elif graph == "activation":
            return request.app.state.activation_graph.get_state(config).values
        else:
            return {
                "error": "graph deve ser: support, logging ou activation",
                "graph_recebido": graph,
                "opcoes_validas": ["support", "logging", "activation"]
            }
    
    except Exception as e:
        return {
            "error": f"Erro ao obter estado do grafo: {str(e)}",
            "thread_id": thread_id,
            "graph": graph
        }

@app.post("/process-logging-queue")
async def process_logging_queue() -> Dict[str, Any]:
    """
    Processa todos os itens da fila de logging
    Acionado sob demanda
    
    Returns:
        Dict com estatísticas do processamento
    """
    print("🔄 Iniciando processamento da fila de logging...")
    
    # Verificar se fila existe
    if not hasattr(app.state, 'logging_queue'):
        raise HTTPException(status_code=500, detail="Fila de logging não inicializada")
    
    if not hasattr(app.state, 'logging_graph'):
        raise HTTPException(status_code=500, detail="Grafo de logging não inicializado")
    
    # Contadores
    processed = 0
    errors = 0
    start_time = asyncio.get_event_loop().time()
    
    # Verificar se fila está vazia
    initial_size = app.state.logging_queue.qsize()
    if initial_size == 0:
        return {
            "message": "Fila vazia - nada para processar",
            "processed": 0,
            "errors": 0,
            "remaining": 0,
            "initial_size": 0,
            "processing_time_ms": 0,
            "status": "empty"
        }
    
    print(f"📦 Processando {initial_size} itens da fila...")
    
    # Processar todos os itens
    while not app.state.logging_queue.empty():
        try:
            # Pegar item da fila (não-bloqueante)
            item = app.state.logging_queue.get_nowait()
            
            print(f"🔄 Processando item {processed + 1}...")
            print(f"🆕 Is new conversation: {item.get('is_new_conversation', 'N/A')}")
            
            # ✅ NOVA LÓGICA: verificar flag
            if item["is_new_conversation"]:
                print(f"🆕 Nova thread - usando ainvoke direto")
                logging_result = await app.state.logging_graph.ainvoke(
                    item["state"], 
                    item["config"]
                )
            else:
                print(f"🔄 Thread existente - usando update_state + ainvoke")
                updated_config = app.state.logging_graph.update_state(
                    item["config"], 
                    {
                        "messages": item["state"]["messages"],
                        "phone_number": item["state"]["phone_number"],
                        "name": item["state"]["name"],
                        "thread_id": item["state"]["thread_id"],
                        "need_classify": item["state"].get("need_classify", False),
                        "structured_response": item['state'].get('structured_response', None)
                    }
                )
                logging_result = await app.state.logging_graph.ainvoke(
                    {}, 
                    updated_config
                )
            
            processed += 1
            print(f"✅ Item {processed} processado com sucesso")
            
            # Marcar como concluído
            app.state.logging_queue.task_done()
            
        except asyncio.QueueEmpty:
            # Fila vazia - sair do loop
            print("📭 Fila esvaziada durante processamento")
            break
            
        except Exception as e:
            errors += 1
            print(f"❌ Erro no item {processed + errors}: {e}")
            
            # Marcar como concluído mesmo com erro
            try:
                app.state.logging_queue.task_done()
            except ValueError:
                # task_done() chamado mais vezes que get()
                pass
    
    # Calcular estatísticas
    end_time = asyncio.get_event_loop().time()
    processing_time_ms = round((end_time - start_time) * 1000, 2)
    remaining = app.state.logging_queue.qsize()
    
    # Resultado final
    result = {
        "message": f"Processamento concluído",
        "processed": processed,
        "errors": errors,
        "remaining": remaining,
        "initial_size": initial_size,
        "processing_time_ms": processing_time_ms,
        "status": "completed" if errors == 0 else "completed_with_errors"
    }
    
    print(f"🎯 Processamento da fila concluído:")
    print(f"   ✅ Processados: {processed}")
    print(f"   ❌ Erros: {errors}")
    print(f"   📦 Restantes: {remaining}")
    print(f"   ⏱️ Tempo: {processing_time_ms}ms")
    
    return result

@app.get("/logging-queue-status")
async def logging_queue_status() -> Dict[str, Any]:
    """
    Retorna status atual da fila de logging
    
    Returns:
        Dict com informações da fila
    """
    # Verificar se fila existe
    if not hasattr(app.state, 'logging_queue'):
        raise HTTPException(status_code=500, detail="Fila de logging não inicializada")
    
    queue_size = app.state.logging_queue.qsize()
    is_empty = app.state.logging_queue.empty()
    
    # Determinar status
    if is_empty:
        status = "empty"
        message = "Fila vazia"
    elif queue_size < 10:
        status = "normal"
        message = f"{queue_size} itens na fila"
    elif queue_size < 50:
        status = "busy"
        message = f"{queue_size} itens na fila - considere processar"
    else:
        status = "overloaded"
        message = f"{queue_size} itens na fila - processamento urgente recomendado"
    
    return {
        "queue_size": queue_size,
        "empty": is_empty,
        "status": status,
        "message": message,
        "max_size": getattr(app.state.logging_queue, 'maxsize', 0),
        "timestamp": int(asyncio.get_event_loop().time() * 1000)
    }

@app.delete("/logging-queue")
async def clear_logging_queue() -> Dict[str, Any]:
    """
    Limpa todos os itens da fila de logging
    ⚠️ CUIDADO: Itens serão perdidos sem processamento
    
    Returns:
        Dict com quantidade de itens removidos
    """
    # Verificar se fila existe
    if not hasattr(app.state, 'logging_queue'):
        raise HTTPException(status_code=500, detail="Fila de logging não inicializada")
    
    initial_size = app.state.logging_queue.qsize()
    
    if initial_size == 0:
        return {
            "message": "Fila já estava vazia",
            "items_removed": 0,
            "status": "empty"
        }
    
    print(f"🗑️ Limpando {initial_size} itens da fila...")
    
    # Remover todos os itens
    removed = 0
    while not app.state.logging_queue.empty():
        try:
            app.state.logging_queue.get_nowait()
            app.state.logging_queue.task_done()
            removed += 1
        except asyncio.QueueEmpty:
            break
    
    print(f"✅ {removed} itens removidos da fila")
    
    return {
        "message": f"{removed} itens removidos da fila",
        "items_removed": removed,
        "initial_size": initial_size,
        "status": "cleared"
    }

@app.get("/worker-status")
async def worker_status() -> Dict[str, Any]:
    """
    Retorna status detalhado do auto-worker e da fila
    """
    try:
        # Status do worker
        worker_running = getattr(app.state, 'auto_worker_running', False)
        worker_task = getattr(app.state, 'auto_worker_task', None)
        
        # Status da task
        task_info = {}
        if worker_task:
            task_info = {
                "exists": True,
                "done": worker_task.done(),
                "cancelled": worker_task.cancelled(),
                "exception": str(worker_task.exception()) if worker_task.done() and worker_task.exception() else None
            }
        else:
            task_info = {"exists": False}
        
        # Status da fila
        queue_info = {}
        if hasattr(app.state, 'logging_queue'):
            queue_info = {
                "exists": True,
                "size": app.state.logging_queue.qsize(),
                "empty": app.state.logging_queue.empty(),
                "maxsize": getattr(app.state.logging_queue, 'maxsize', 0)
            }
        else:
            queue_info = {"exists": False}
        
        return {
            "timestamp": int(time.time() * 1000),
            "worker": {
                "running_flag": worker_running,
                "task": task_info
            },
            "queue": queue_info,
            "status": "healthy" if worker_running and task_info.get("exists") and not task_info.get("done") else "unhealthy"
        }
        
    except Exception as e:
        return {
            "timestamp": int(time.time() * 1000),
            "error": str(e),
            "status": "error"
        }

######## ATIVAÇÃO ########################

async def _activation_service(context: Dict[str, Any], activation_graph: Any, app_state: Any) -> Dict[str, Any]:
    """
    ActivationService - Integração com o Activation_Graph compilado.
    Uncle Bob: 'Business logic separated from delivery mechanism'
    
    IMPORTANTE: Usa EXATAMENTE o mesmo mecanismo de thread do /agent
    """
    print(f"[ACTIVATION_SERVICE] 📥 Processando {context['flow']}")
    print(f"[ACTIVATION_SERVICE] 📱 Phone: {context['phone_number']}")
    
    try:
        phone_number = context["phone_number"]
        
        # ===== USAR MESMO MECANISMO DE THREAD DO /agent =====
        
        # Verificar inicialização de estado (igual ao /agent)
        if not hasattr(app_state, 'threads'):
            app_state.threads = {}
        if not hasattr(app_state, 'last_thread_activity'):
            app_state.last_thread_activity = {}
        
        # ✅ USAR A FUNÇÃO should_create_new_thread QUE JÁ EXISTE
        # TODO: Importar a função should_create_new_thread do módulo correto
        # Por enquanto, lógica simplificada baseada no /agent:
        
        current_time = int(time.time() * 1000)
        should_create_new = phone_number not in app_state.threads
        
        if should_create_new:
            # Criar nova thread (usando generate_uuid como no /agent)
            # TODO: Importar generate_uuid do módulo correto
            import uuid
            thread_id = str(uuid.uuid4())  # Placeholder - usar generate_uuid() real
            app_state.threads[phone_number] = thread_id
            app_state.last_thread_activity[phone_number] = current_time
            
            print(f"[ACTIVATION_SERVICE] 🆕 Nova thread criada: {thread_id}")
            is_new_conversation = True
        else:
            # Usar thread existente
            thread_id = app_state.threads[phone_number]
            app_state.last_thread_activity[phone_number] = current_time
            
            print(f"[ACTIVATION_SERVICE] ♻️ Usando thread existente: {thread_id}")
            is_new_conversation = False
        
        thread_config = {"configurable": {"thread_id": thread_id}}
        
        print(f"[ACTIVATION_SERVICE] 🧵 Thread: {thread_id} (nova: {is_new_conversation})")
        
        # ===== PREPARAR ESTADO INICIAL DO GRAFO =====
        initial_state = {
            "messages": [],
            "phone_number": context["phone_number"],
            "name": "Sistema",
            "flow_name": context["flow"],
            "activation_context": [context["informacoes_contexto"]]
        }
        
        print(f"[ACTIVATION_SERVICE] 🔧 Estado inicial preparado")
        print(f"[ACTIVATION_SERVICE] 📋 Flow: {context['flow']}")
        print(f"[ACTIVATION_SERVICE] 🧵 Thread Config: {thread_config}")
        
        # ===== INVOCAR ACTIVATION_GRAPH =====
        print(f"[ACTIVATION_SERVICE] 🚀 Invocando activation_graph...")
        
        graph_result = await activation_graph.ainvoke(initial_state, thread_config)
        
        print(f"[ACTIVATION_SERVICE] ✅ Grafo executado com sucesso")
        print(f"[ACTIVATION_SERVICE] 📊 Resultado tipo: {type(graph_result)}")
        print(f"[ACTIVATION_SERVICE] 📊 Keys do resultado: {list(graph_result.keys()) if isinstance(graph_result, dict) else 'N/A'}")
        
        # ===== PROCESSAR RESULTADO =====
        return {
            "status": "success",
            "message": f"Flow {context['flow']} executado com sucesso",
            "thread_id": thread_id,
            "is_new_conversation": is_new_conversation,
            "flow_executed": context['flow'],
            "graph_result": graph_result if isinstance(graph_result, dict) else str(graph_result),
            "context_processed": True,
            "timestamp": int(time.time() * 1000)
        }
        
    except Exception as e:
        print(f"[ACTIVATION_SERVICE] ❌ Erro ao executar grafo: {e}")
        print(f"[ACTIVATION_SERVICE] ❌ Traceback: {traceback.format_exc()}")
        
        return {
            "status": "error",
            "message": f"Erro ao executar flow {context['flow']}",
            "error": str(e),
            "flow": context['flow'],
            "timestamp": int(time.time() * 1000)
        }

# Router para ativações (seguindo padrão do projeto)
activation_router = APIRouter(tags=["activation"])

class ActivationTriggerPayload(BaseModel):
    phone_number: str = Field(..., description="Número de telefone no formato brasileiro completo")
    flow: str = Field(..., description="Nome do fluxo de ativação a ser executado")
    informacoes_contexto: Dict[str, Any] = Field(default_factory=dict, description="Contexto livre para personalização da mensagem")

@activation_router.post("/trigger")
async def trigger_activation(payload: ActivationTriggerPayload, request: Request):
    """
    Endpoint para disparar fluxos de ativação manual.
    
    Fluxo:
    1. Recebe trigger com phone_number + flow + contexto
    2. Valida dados básicos
    3. Chama ActivationService (que usa activation_graph)
    4. Activation_Graph → Sub-grafo → thread → /send
    5. Retorna status da ativação
    """
    try:
        start_time = time.time()
        
        print(f"\n[TRIGGER] 🎯 === NOVA ATIVAÇÃO INICIADA ===")
        print(f"[TRIGGER] 📱 Phone: {payload.phone_number}")
        print(f"[TRIGGER] 🔄 Flow: {payload.flow}")
        print(f"[TRIGGER] 📋 Contexto: {payload.informacoes_contexto}")
        
        # ===== VALIDAÇÕES BÁSICAS =====
        
        # Validar formato do telefone (brasileiro)
        if not payload.phone_number.startswith('55'):
            return JSONResponse(
                status_code=400, 
                content={"error": "Número deve começar com 55 (código do Brasil)"}
            )
        
        if len(payload.phone_number) not in [12, 13]:  # 55 + DDD + 8/9 dígitos
            return JSONResponse(
                status_code=400,
                content={"error": "Formato de telefone inválido"}
            )
        
        # Validar se flow é reconhecido (lista básica para começar)
        flows_validos = [
            "ativacao_masterclass",
        ]
        
        if payload.flow not in flows_validos:
            return JSONResponse(
                status_code=400,
                content={
                    "error": f"Flow '{payload.flow}' não reconhecido",
                    "flows_validos": flows_validos
                }
            )
        
        print(f"[TRIGGER] ✅ Validações passaram")
        
        # ===== PREPARAR CONTEXTO PARA ACTIVATION_GRAPH =====
        
        activation_context = {
            "phone_number": payload.phone_number,
            "flow": payload.flow,
            "informacoes_contexto": payload.informacoes_contexto,
            "trigger_timestamp": int(time.time() * 1000),
            "trigger_source": "manual_endpoint"
        }
        
        print(f"[TRIGGER] 🔧 Contexto preparado para Activation_Graph")
        
        # ===== DELEGAR PARA CAMADA DE NEGÓCIO =====
        
        print(f"[TRIGGER] 🚀 Delegando para ActivationService...")
        
        # Usar o grafo compilado do app.state
        activation_result = await _activation_service(
            context=activation_context,
            activation_graph=request.app.state.activation_graph,
            app_state=request.app.state  # ← Passar app_state para gerenciar threads
        )
        
        print(f"[TRIGGER] ✅ ActivationService respondeu")
        print(f"[TRIGGER] 📊 Resultado: {activation_result['status']}")
        
        # ===== PREPARAR RESPOSTA =====
        
        end_time = time.time()
        processing_time = round((end_time - start_time) * 1000, 2)
        
        response = {
            "status": "success",
            "message": "Ativação disparada com sucesso",
            "trigger_data": {
                "phone_number": payload.phone_number,
                "flow": payload.flow,
                "contexto_size": len(payload.informacoes_contexto),
                "timestamp": int(time.time() * 1000)
            },
            "activation_result": activation_result,
            "processing_time_ms": processing_time
        }
        
        print(f"[TRIGGER] 🎉 Ativação concluída em {processing_time}ms")
        print(f"[TRIGGER] === FIM DA ATIVAÇÃO ===\n")
        
        return JSONResponse(content=response)
        
    except Exception as e:
        print(f"[TRIGGER] ❌ Erro crítico: {e}")
        print(f"[TRIGGER] ❌ Traceback: {traceback.format_exc()}")
        
        return JSONResponse(
            status_code=500,
            content={
                "error": str(e),
                "message": "Erro interno no trigger de ativação",
                "timestamp": int(time.time() * 1000)
            }
        )

@activation_router.post("/test-image-activation")
async def test_image_activation(request: Request):
    """
    Endpoint SIMPLES para testar envio de imagem com fluxo de ativação
    
    Envia imagem fixa para número fixo com flow fixo para teste
    """
    try:
        start_time = time.time()
        
        print(f"\n[TEST-IMAGE] 🧪 === TESTE DE IMAGEM COM ATIVAÇÃO ===")
        
        # ===== DADOS FIXOS PARA TESTE =====
        phone_number = "5531995655690"
        image_path = "/app/guardia_engine_test/aniversarios_folder.jpg"
        flow_name = "aniversarios"
        legenda = """🎉 FELIZ ANIVERSÁRIO! 🎂

Hoje é um dia muito especial! 🥳

Desejamos que este novo ano de vida seja repleto de alegrias, conquistas e momentos inesquecíveis!

🎁 Que todos os seus sonhos se realizem!
🌟 Muito sucesso e felicidade sempre!

Com carinho,
Equipe guardIA 💙"""
        
        print(f"[TEST-IMAGE] 📱 Phone: {phone_number}")
        print(f"[TEST-IMAGE] 🖼️ Image: {image_path}")
        print(f"[TEST-IMAGE] 🎯 Flow: {flow_name}")
        print(f"[TEST-IMAGE] 📝 Legenda: {legenda[:50]}...")
        
        # ===== OBTER DADOS DA EVOLUTION API =====
        instance_name = os.getenv("WHATSAPP_API_INSTANCE")
        api_key = os.getenv("WHATSAPP_API_TOKEN")
        api_url = os.getenv("WHATSAPP_API_URL", "https://api.evolution.com")
        
        if not instance_name or not api_key:
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Configuração incompleta",
                    "message": "WHATSAPP_API_INSTANCE ou WHATSAPP_API_TOKEN não configurados"
                }
            )
        
        print(f"[TEST-IMAGE] 🔧 Instance: {instance_name}")
        print(f"[TEST-IMAGE] 🔧 API URL: {api_url}")
        
        # ===== VERIFICAR SE ARQUIVO EXISTE =====
        if not os.path.exists(image_path):
            return JSONResponse(
                status_code=404,
                content={
                    "error": "Arquivo não encontrado",
                    "image_path": image_path,
                    "message": "Verifique se o arquivo existe no caminho especificado"
                }
            )
        
        print(f"[TEST-IMAGE] ✅ Arquivo encontrado: {os.path.getsize(image_path)} bytes")
        
        # ===== OBTER CLIENTE HTTP E APP STATE =====
        http_client = request.app.state.http_client
        app_state = request.app.state
        
        # Inicializar activation_cache se não existir
        if not hasattr(app_state, 'activation_cache'):
            app_state.activation_cache = {}
            print(f"[TEST-IMAGE] 🔧 activation_cache inicializado")
        
        print(f"[TEST-IMAGE] 🔧 Cliente HTTP e app_state obtidos")
        
        # ===== CHAMAR A FUNÇÃO DE ENVIO =====
        print(f"[TEST-IMAGE] 🚀 Enviando imagem com fluxo de ativação...")
        
        # TODO: Importar sua função send_base64_image_async
        # from seu_modulo import send_base64_image_async
        
        result = await send_base64_image_async(
            url_api_whatsapp=api_url,
            instancia=instance_name,
            api_key=api_key,
            numero=phone_number,
            caminho_imagem=image_path,
            legenda=legenda,
            http_client=http_client,
            # 🎯 PARÂMETROS DE ATIVAÇÃO
            app_state=app_state,
            is_activation_flow=True,
            flow_name=flow_name
        )
        
        # ===== PROCESSAR RESULTADO =====
        end_time = time.time()
        processing_time = round((end_time - start_time) * 1000, 2)
        
        if result:
            print(f"[TEST-IMAGE] ✅ Sucesso! Imagem enviada em {processing_time}ms")
            
            response = {
                "status": "success",
                "message": "Imagem de teste enviada com sucesso",
                "test_data": {
                    "phone_number": phone_number,
                    "image_path": image_path,
                    "flow_name": flow_name,
                    "legenda_size": len(legenda),
                    "file_size_bytes": os.path.getsize(image_path)
                },
                "send_result": result,
                "activation_flow": True,
                "processing_time_ms": processing_time,
                "timestamp": int(time.time() * 1000)
            }
            
            print(f"[TEST-IMAGE] 🎉 Teste concluído com sucesso!")
            print(f"[TEST-IMAGE] === FIM DO TESTE ===\n")
            
            return JSONResponse(content=response)
        
        else:
            print(f"[TEST-IMAGE] ❌ Falha no envio da imagem")
            
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "message": "Falha no envio da imagem",
                    "test_data": {
                        "phone_number": phone_number,
                        "image_path": image_path,
                        "flow_name": flow_name
                    },
                    "processing_time_ms": processing_time,
                    "timestamp": int(time.time() * 1000)
                }
            )
        
    except Exception as e:
        print(f"[TEST-IMAGE] ❌ Erro crítico: {e}")
        print(f"[TEST-IMAGE] ❌ Traceback: {traceback.format_exc()}")
        
        return JSONResponse(
            status_code=500,
            content={
                "error": str(e),
                "message": "Erro interno no teste de imagem",
                "timestamp": int(time.time() * 1000)
            }
        )


####  SCHEDULER ##########

# ==========================================
# 1. MODELOS PYDANTIC (OPCIONAL)
# ==========================================

class TaskResponse(BaseModel):
    id: str
    name: Optional[str]
    next_run: Optional[str]
    trigger_type: str
    function_name: str
    args: List
    kwargs: Dict[str, Any]

class TaskListResponse(BaseModel):
    total_tasks: int
    tasks: List[TaskResponse]
    timestamp: str

class CancelTaskRequest(BaseModel):
    task_id: str
    reason: Optional[str] = "Manual cancellation"

class GenericTaskScheduleRequest(BaseModel):
    target_datetime: str = Field(..., description="Data/hora de execução no formato ISO: 2024-12-25T09:00:00")
    timezone: str = Field(default="America/Sao_Paulo", description="Fuso horário")
    task_type: str = Field(..., description="Tipo de task: 'http_request'")
    task_id: Optional[str] = Field(default=None, description="ID personalizado (opcional)")
    description: Optional[str] = Field(default="", description="Descrição da task")
    task_params: Dict[str, Any] = Field(..., description="Parâmetros específicos do tipo de task")


# ==========================================
# 2. FUNÇÕES AUXILIARES
# ==========================================

def execute_http_request_task(**params):
    """
    Executa uma requisição HTTP
    
    Parâmetros esperados em task_params:
    - url: URL do endpoint
    - method: GET, POST, etc. (default: POST)
    - payload: Dados JSON (opcional)
    - headers: Cabeçalhos HTTP (opcional)
    """

    
    url = params.get('url')
    method = params.get('method', 'POST')
    payload = params.get('payload', {})
    headers = params.get('headers', {'Content-Type': 'application/json'})
    
    print(f"🌐 [HTTP-TASK] Executando {method} {url}")
    print(f"🌐 [HTTP-TASK] Payload: {payload}")
    
    try:
        if method.upper() == 'GET':
            response = requests.get(url, headers=headers, params=payload, timeout=30)
        elif method.upper() == 'POST':
            response = requests.post(url, headers=headers, json=payload, timeout=30)
        elif method.upper() == 'PUT':
            response = requests.put(url, headers=headers, json=payload, timeout=30)
        elif method.upper() == 'DELETE':
            response = requests.delete(url, headers=headers, timeout=30)
        else:
            print(f"❌ [HTTP-TASK] Método {method} não suportado")
            return False
        
        print(f"✅ [HTTP-TASK] Status: {response.status_code}")
        print(f"✅ [HTTP-TASK] Response: {response.text[:200]}...")
        
        return response.status_code < 400
        
    except Exception as e:
        print(f"❌ [HTTP-TASK] Erro: {e}")
        return False

def execute_generic_task(task_type: str, task_params: Dict[str, Any], task_id: str):
    """
    Função genérica que o APScheduler vai chamar
    """
    print(f"\n🚀 [GENERIC-TASK] Executando task: {task_id}")
    print(f"🚀 [GENERIC-TASK] Tipo: {task_type}")
    print(f"🚀 [GENERIC-TASK] Timestamp: {datetime.now()}")
    
    # Mapeamento de executores
    TASK_EXECUTORS = {
        'http_request': execute_http_request_task,
    }
    
    # Buscar executor correto
    executor = TASK_EXECUTORS.get(task_type)
    
    if not executor:
        print(f"❌ [GENERIC-TASK] Tipo de task '{task_type}' não suportado")
        print(f"❌ [GENERIC-TASK] Tipos disponíveis: {list(TASK_EXECUTORS.keys())}")
        return False
    
    try:
        # Executar task
        success = executor(**task_params)
        
        if success:
            print(f"✅ [GENERIC-TASK] Task {task_id} executada com sucesso!")
        else:
            print(f"❌ [GENERIC-TASK] Task {task_id} falhou na execução")
        
        return success
        
    except Exception as e:
        print(f"❌ [GENERIC-TASK] Erro crítico na execução da task {task_id}: {e}")
        print(f"❌ [GENERIC-TASK] Traceback: {traceback.format_exc()}")
        return False

def get_scheduler_from_request(request: Request):
    """
    Extrai o scheduler do estado da aplicação
    """
    if hasattr(request.app.state, 'scheduler'):
        return request.app.state.scheduler
    
    raise HTTPException(
        status_code=500, 
        detail="Scheduler não encontrado no app.state"
    )

def format_task_info(job) -> Dict[str, Any]:
    """
    Formata informações de uma task para resposta da API
    """
    try:
        return {
            "id": job.id,
            "name": job.name or "Unnamed Task",
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger_type": type(job.trigger).__name__,
            "function_name": f"{job.func.__module__}.{job.func.__name__}" if hasattr(job.func, '__module__') else str(job.func),
            "args": list(job.args) if job.args else [],
            "kwargs": dict(job.kwargs) if job.kwargs else {},
            "misfire_grace_time": job.misfire_grace_time,
            "max_instances": job.max_instances
        }
    except Exception as e:
        return {
            "id": getattr(job, 'id', 'unknown'),
            "name": "Error parsing task",
            "error": str(e),
            "next_run": None,
            "trigger_type": "unknown",
            "function_name": "unknown",
            "args": [],
            "kwargs": {}
        }

# ==========================================
# 3. ROUTER PARA TASKS
# ==========================================

@tasks_router.post("/schedule", summary="Agendar task genérica")
async def schedule_generic_task(
    request_data: GenericTaskScheduleRequest, 
    request: Request
) -> Dict[str, Any]:
    """
    Endpoint genérico para agendar tasks HTTP
    
    Exemplo de uso:
    ```json
    {
      "target_datetime": "2024-12-25T09:00:00",
      "task_type": "http_request",
      "description": "Aniversário do João",
      "task_params": {
        "url": "http://212.85.1.27:8009/activation/trigger",
        "method": "POST",
        "payload": {
          "phone_number": "5531995655690",
          "flow": "aniversarios",
          "informacoes_contexto": {"nome_pessoa": "João"}
        }
      }
    }
    ```
    """
    try:
        start_time = datetime.now()
        
        print(f"\n📅 [SCHEDULE-GENERIC] Nova solicitação de agendamento")
        print(f"📅 [SCHEDULE-GENERIC] Tipo: {request_data.task_type}")
        print(f"📅 [SCHEDULE-GENERIC] Para: {request_data.target_datetime}")
        
        # Obter scheduler
        try:
            scheduler = get_scheduler_from_request(request)
        except HTTPException as e:
            return JSONResponse(
                status_code=e.status_code,
                content={
                    "status": "error",
                    "error": e.detail,
                    "timestamp": start_time.isoformat()
                }
            )
        
        # Validar tipo de task
        if request_data.task_type != 'http_request':
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "error": f"Tipo de task '{request_data.task_type}' não suportado",
                    "supported_types": ["http_request"],
                    "timestamp": start_time.isoformat()
                }
            )
        
        # Gerar ID se não fornecido
        if not request_data.task_id:
            import uuid
            request_data.task_id = f"{request_data.task_type}_{uuid.uuid4().hex[:8]}"
        
        # Converter string para datetime
        try:
            import pytz
            tz = pytz.timezone(request_data.timezone)
            
            # Parse da data
            target_dt = datetime.fromisoformat(request_data.target_datetime.replace('Z', '+00:00'))
            
            # Aplicar timezone se não tiver
            if target_dt.tzinfo is None:
                target_dt = tz.localize(target_dt)
                
        except Exception as e:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error", 
                    "error": f"Formato de data inválido: {str(e)}",
                    "expected_format": "2024-12-25T09:00:00",
                    "timestamp": start_time.isoformat()
                }
            )
        
        # Verificar se data não é no passado
        now = datetime.now(tz)
        if target_dt <= now:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "error": "Data/hora deve ser no futuro",
                    "provided": target_dt.isoformat(),
                    "current": now.isoformat(),
                    "timestamp": start_time.isoformat()
                }
            )
        
        # Agendar task
        try:
            job = scheduler.add_job(
                func=execute_generic_task,
                trigger='date',
                run_date=target_dt,
                args=[request_data.task_type, request_data.task_params, request_data.task_id],
                id=request_data.task_id,
                name=request_data.description or f"{request_data.task_type} task",
                replace_existing=True
            )
            
            print(f"✅ [SCHEDULE-GENERIC] Task agendada com sucesso!")
            print(f"✅ [SCHEDULE-GENERIC] ID: {request_data.task_id}")
            print(f"✅ [SCHEDULE-GENERIC] Próxima execução: {job.next_run_time}")
            
            # Resposta de sucesso
            return JSONResponse(content={
                "status": "success",
                "task_id": request_data.task_id,
                "scheduled_for": target_dt.isoformat(),
                "task_type": request_data.task_type,
                "message": f"Task '{request_data.task_id}' agendada com sucesso",
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "description": request_data.description,
                "timezone": request_data.timezone
            })
            
        except Exception as e:
            print(f"❌ [SCHEDULE-GENERIC] Erro ao agendar: {e}")
            
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "error": f"Erro ao agendar task: {str(e)}",
                    "task_id": request_data.task_id,
                    "timestamp": start_time.isoformat()
                }
            )
        
    except Exception as e:
        print(f"❌ [SCHEDULE-GENERIC] Erro crítico: {e}")
        print(f"❌ [SCHEDULE-GENERIC] Traceback: {traceback.format_exc()}")
        
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "error": str(e),
                "message": "Erro interno no agendamento",
                "timestamp": datetime.now().isoformat()
            }
        )

@tasks_router.get("/list", summary="Listar todas as tasks agendadas")
async def list_scheduled_tasks(request: Request) -> Dict[str, Any]:
    """
    Lista todas as tasks agendadas no APScheduler
    
    Returns:
        - Lista com informações detalhadas de cada task
        - Total de tasks
        - Timestamp da consulta
    """
    try:
        start_time = datetime.now()
        
        # Obter scheduler
        try:
            scheduler = get_scheduler_from_request(request)
        except HTTPException as e:
            return JSONResponse(
                status_code=e.status_code,
                content={
                    "error": e.detail,
                    "suggestion": "Certifique-se de salvar o scheduler em app.state.scheduler na inicialização",
                    "timestamp": start_time.isoformat()
                }
            )
        
        # Obter todas as tasks
        jobs = scheduler.get_jobs()
        
        print(f"📋 [LIST-TASKS] Encontradas {len(jobs)} tasks agendadas")
        
        # Formatar informações das tasks
        tasks_info = []
        errors = []
        
        for job in jobs:
            try:
                task_info = format_task_info(job)
                tasks_info.append(task_info)
                
                # Log básico
                print(f"   🔹 {job.id} → {job.next_run_time}")
                
            except Exception as e:
                error_info = {
                    "job_id": getattr(job, 'id', 'unknown'),
                    "error": str(e)
                }
                errors.append(error_info)
                print(f"   ❌ Erro ao processar task {getattr(job, 'id', 'unknown')}: {e}")
        
        # Ordenar por próxima execução
        tasks_info.sort(key=lambda x: x['next_run'] or '9999-12-31T23:59:59')
        
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds() * 1000
        
        response = {
            "status": "success",
            "total_tasks": len(tasks_info),
            "tasks": tasks_info,
            "processing_time_ms": round(processing_time, 2),
            "timestamp": start_time.isoformat(),
            "scheduler_running": scheduler.running if hasattr(scheduler, 'running') else True
        }
        
        # Adicionar erros se houver
        if errors:
            response["errors"] = errors
            response["warning"] = f"{len(errors)} tasks tiveram erro ao ser processadas"
        
        print(f"📊 [LIST-TASKS] Resposta preparada em {processing_time:.2f}ms")
        
        return JSONResponse(content=response)
        
    except Exception as e:
        print(f"❌ [LIST-TASKS] Erro crítico: {e}")
        print(f"❌ [LIST-TASKS] Traceback: {traceback.format_exc()}")
        
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "error": str(e),
                "message": "Erro interno ao listar tasks",
                "timestamp": datetime.now().isoformat()
            }
        )

@tasks_router.delete("/cancel/{task_id}", summary="Cancelar task específica")
async def cancel_scheduled_task(task_id: str, request: Request) -> Dict[str, Any]:
    """
    Cancela uma task agendada específica
    
    Args:
        task_id: ID da task a ser cancelada
        
    Returns:
        Status da operação de cancelamento
    """
    try:
        start_time = datetime.now()
        
        print(f"🗑️ [CANCEL-TASK] Solicitação para cancelar: {task_id}")
        
        # Obter scheduler
        try:
            scheduler = get_scheduler_from_request(request)
        except HTTPException as e:
            return JSONResponse(
                status_code=e.status_code,
                content={
                    "error": e.detail,
                    "task_id": task_id,
                    "timestamp": start_time.isoformat()
                }
            )
        
        # Verificar se task existe antes de cancelar
        try:
            job = scheduler.get_job(task_id)
            if job is None:
                print(f"⚠️ [CANCEL-TASK] Task {task_id} não encontrada")
                return JSONResponse(
                    status_code=404,
                    content={
                        "status": "not_found",
                        "message": f"Task '{task_id}' não encontrada",
                        "task_id": task_id,
                        "timestamp": start_time.isoformat()
                    }
                )
            
            # Obter informações da task antes de cancelar
            task_info = format_task_info(job)
            
        except Exception as e:
            print(f"❌ [CANCEL-TASK] Erro ao obter info da task {task_id}: {e}")
            task_info = {"id": task_id, "error": "Could not retrieve task info"}
        
        # Cancelar task
        try:
            scheduler.remove_job(task_id)
            
            print(f"✅ [CANCEL-TASK] Task {task_id} cancelada com sucesso")
            
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds() * 1000
            
            return JSONResponse(content={
                "status": "success",
                "message": f"Task '{task_id}' cancelada com sucesso",
                "task_id": task_id,
                "cancelled_task_info": task_info,
                "processing_time_ms": round(processing_time, 2),
                "timestamp": start_time.isoformat()
            })
            
        except Exception as cancel_error:
            print(f"❌ [CANCEL-TASK] Erro ao cancelar {task_id}: {cancel_error}")
            
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "message": f"Erro ao cancelar task '{task_id}': {str(cancel_error)}",
                    "task_id": task_id,
                    "error": str(cancel_error),
                    "timestamp": start_time.isoformat()
                }
            )
        
    except Exception as e:
        print(f"❌ [CANCEL-TASK] Erro crítico: {e}")
        print(f"❌ [CANCEL-TASK] Traceback: {traceback.format_exc()}")
        
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "error": str(e),
                "message": "Erro interno ao cancelar task",
                "task_id": task_id,
                "timestamp": datetime.now().isoformat()
            }
        )

@tasks_router.get("/scheduler-status", summary="Status do scheduler")
async def scheduler_status(request: Request) -> Dict[str, Any]:
    """
    Retorna informações sobre o status do scheduler
    """
    try:
        # Obter scheduler
        try:
            scheduler = get_scheduler_from_request(request)
        except HTTPException as e:
            return JSONResponse(
                status_code=e.status_code,
                content={
                    "error": e.detail,
                    "timestamp": datetime.now().isoformat()
                }
            )
        
        # Coletar informações
        total_jobs = len(scheduler.get_jobs())
        is_running = scheduler.running if hasattr(scheduler, 'running') else True
        
        return JSONResponse(content={
            "scheduler_running": is_running,
            "total_jobs": total_jobs,
            "timezone": str(scheduler.timezone) if hasattr(scheduler, 'timezone') else 'unknown',
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
        )


####### CONTEXT ##########################

class WhatsAppRequest(BaseModel):
    # Obrigatórios
    numero_telefone: str
    
    # Opcionais com valores padrão
    lista_contatos: Optional[List[Dict[str, Any]]] = None
    formato_saida: str = "texto"
    quebrar_conversas: bool = False
    modo_quebra: str = "tempo_inteligente"
    intervalo_max_horas: int = 8
    horario_comercial: List[int] = [8, 18]
    dias_uteis: List[int] = [0,1,2,3,4,5,6]
    limite_por_pagina: int = 200  # MUDOU: de 50 para 200
    max_paginas: int = 100        # MANTÉM: já permite buscar 20.000 mensagens (200 x 100)
    timeout: int = 30
    data_hora_inicio: Optional[str] = None
    data_hora_fim: Optional[str] = None
    forcar_evolution_apenas: bool = False

class TelefonesRequest(BaseModel):
    # Opcionais com valores padrão (se não informado, pega período amplo)
    data_hora_inicio: Optional[str] = None
    data_hora_fim: Optional[str] = None

# AnaliseQuantitativaRequest
class AnaliseQuantitativaRequest(BaseModel):
    numero_telefone: str = Field(..., description="Número de telefone para análise", example="5531995655690")
    data_hora_inicio: Optional[str] = Field(None, description="Data/hora início (opcional)", example="2024-12-01 00:00:00")
    data_hora_fim: Optional[str] = Field(None, description="Data/hora fim (opcional)", example="2024-12-31 23:59:59")
    teto_minutos: Optional[int] = Field(30, description="Teto em minutos para tempo de trabalho", example=30)
    forcar_evolution_apenas: bool = Field(False, description="Se True, busca apenas Evolution API")

class ContextoNegocio(BaseModel):
    """Modelo para contexto de negócio com defaults do .env"""
    nome_empresa: str = Field(default_factory=lambda: os.getenv("CONTEXTO_NOME_EMPRESA", "Empresa"))
    tipo_negocio: str = Field(default_factory=lambda: os.getenv("CONTEXTO_TIPO_NEGOCIO", "empresa"))
    especialidade: str = Field(default_factory=lambda: os.getenv("CONTEXTO_ESPECIALIDADE", "serviços diversos"))
    profissionais_principais: List[str] = Field(default_factory=lambda: os.getenv("CONTEXTO_PROFISSIONAIS", "Atendente").split(","))
    cidade: str = Field(default_factory=lambda: os.getenv("CONTEXTO_CIDADE", "Não especificado"))
    responsavel_atendimento: str = Field(default_factory=lambda: os.getenv("CONTEXTO_RESPONSAVEL_ATENDIMENTO", "Atendente"))
    numero_whatsapp: str = Field(default_factory=lambda: os.getenv("CONTEXTO_NUMERO_WHATSAPP", "Não especificado"))
    descricao_servicos: str = Field(default_factory=lambda: os.getenv("CONTEXTO_DESCRICAO_SERVICOS", "Serviços diversos"))

class AnaliseQualitativaRequest(BaseModel):
    numero_telefone: str
    data_hora_inicio: Optional[str] = None
    data_hora_fim: Optional[str] = None
    campos_analise: Optional[List[str]] = None
    contexto_negocio: Optional[ContextoNegocio] = None
    modelo: Optional[str] = None
    temperatura: float = 0.1
    forcar_evolution_apenas: bool = False

class AnaliseCompletaRequest(BaseModel):
    numero_telefone: str
    data_hora_inicio: Optional[str] = None
    data_hora_fim: Optional[str] = None
    teto_minutos: Optional[int] = 30
    temperatura: Optional[float] = 0.1
    modelo: Optional[str] = "sabia-3.1"
    forcar_evolution_apenas: bool = False

context_router = APIRouter(tags=["context"])

@context_router.post("/extrair-telefones-conversas-periodo-com-ia")
async def endpoint_listar_telefones_com_ia(request: TelefonesRequest):
    """
    📋🤖 Lista telefones que tiveram pelo menos 1 mensagem de IA
    """
    
    logger = logging.getLogger(__name__)
    
    # === VARIÁVEIS DE AMBIENTE ===
    baserow_token = os.getenv("BASEROW_API_TOKEN")
    baserow_message_table_id = os.getenv("BASEROW_MESSAGE_TABLE_ID")
    
    if not baserow_token or not baserow_message_table_id:
        raise HTTPException(status_code=500, detail="Variáveis de ambiente não definidas")
    
    try:
        message_table_id = int(baserow_message_table_id)
    except ValueError:
        raise HTTPException(status_code=500, detail="BASEROW_MESSAGE_TABLE_ID deve ser um número inteiro")
    
    # === VALIDAÇÕES ===
    if request.data_hora_inicio:
        try:
            datetime.strptime(request.data_hora_inicio, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            raise HTTPException(status_code=400, detail="data_hora_inicio formato: 'YYYY-MM-DD HH:MM:SS'")
    
    if request.data_hora_fim:
        try:
            datetime.strptime(request.data_hora_fim, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            raise HTTPException(status_code=400, detail="data_hora_fim formato: 'YYYY-MM-DD HH:MM:SS'")
    
    # === FUNÇÕES INTERNAS ===
    
    def buscar_tabela_baserow():
        """Busca toda a tabela do Baserow"""
        base_url = "https://baserow-baserow.hqgf38.easypanel.host"
        headers = {"Authorization": f"Token {baserow_token}"}
        
        all_records = []
        page = 1
        
        logger.info(f"🔍 Buscando dados da tabela {message_table_id}...")
        
        while True:
            url = f"{base_url}/api/database/rows/table/{message_table_id}/"
            params = {"user_field_names": "true", "size": 200, "page": page}
            
            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                results = data.get('results', [])
                
                if not results:
                    break
                    
                all_records.extend(results)
                logger.info(f"📄 Página {page}: +{len(results)} registros")
                
                if not data.get('next'):
                    break
                    
                page += 1
                
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Erro: {e}")
                break
        
        if all_records:
            df = pd.DataFrame(all_records)
            logger.info(f"✅ DataFrame criado: {len(df)} linhas x {len(df.columns)} colunas")
            return df
        else:
            logger.warning("❌ Nenhum dado encontrado")
            return pd.DataFrame()
    
    def filtrar_por_periodo(df):
        """Filtra DataFrame por período brasileiro"""
        if not request.data_hora_inicio or not request.data_hora_fim:
            logger.info("📅 Sem filtro de período")
            return df
        
        # Converter ISO para formato brasileiro
        dt_inicio = datetime.strptime(request.data_hora_inicio, '%Y-%m-%d %H:%M:%S')
        dt_fim = datetime.strptime(request.data_hora_fim, '%Y-%m-%d %H:%M:%S')
        
        data_inicio = dt_inicio.strftime('%d/%m/%Y')
        hora_inicio = dt_inicio.strftime('%H:%M')
        data_fim = dt_fim.strftime('%d/%m/%Y')
        hora_fim = dt_fim.strftime('%H:%M')
        
        # Converter para timestamps UTC
        br_tz = pytz.timezone('America/Sao_Paulo')
        
        start_br = datetime.strptime(f"{data_inicio} {hora_inicio}", '%d/%m/%Y %H:%M')
        start_br_tz = br_tz.localize(start_br)
        start_utc = start_br_tz.astimezone(pytz.UTC)
        start_timestamp_ms = int(start_utc.timestamp() * 1000)
        
        end_br = datetime.strptime(f"{data_fim} {hora_fim}", '%d/%m/%Y %H:%M')
        end_br_tz = br_tz.localize(end_br)
        end_utc = end_br_tz.astimezone(pytz.UTC)
        end_timestamp_ms = int(end_utc.timestamp() * 1000)
        
        logger.info(f"📅 Filtro período: {data_inicio} {hora_inicio} até {data_fim} {hora_fim}")
        
        # Aplicar filtro
        df_copy = df.copy()
        df_copy['messageTimestamp'] = pd.to_numeric(df_copy['messageTimestamp'], errors='coerce')
        
        mask_periodo = (
            (df_copy['messageTimestamp'].notna()) &
            (df_copy['messageTimestamp'] >= start_timestamp_ms) & 
            (df_copy['messageTimestamp'] <= end_timestamp_ms)
        )
        
        df_filtrado = df_copy[mask_periodo]
        logger.info(f"✅ Após filtro: {len(df_filtrado)}/{len(df)} mensagens")
        
        return df_filtrado
    
    def identificar_mensagens_ia(df):
        """Identifica mensagens da IA pelo campo type"""
        if df.empty:
            logger.warning("⚠️ DataFrame vazio")
            return df
        
        if 'type' not in df.columns:
            logger.error("❌ Campo 'type' não encontrado")
            return pd.DataFrame()
        
        logger.info(f"🔍 Analisando {len(df)} mensagens")
        
        # Função para verificar se é AIMessage
        def is_ai_message(type_field):
            if pd.isna(type_field):
                return False
            if isinstance(type_field, dict):
                return type_field.get('value') == 'AIMessage'
            if isinstance(type_field, str):
                return type_field == 'AIMessage'
            return False
        
        # Aplicar filtro
        mask_ia = df['type'].apply(is_ai_message)
        mensagens_ia = df[mask_ia]
        
        logger.info(f"🤖 Mensagens IA encontradas: {len(mensagens_ia)}")
        
        return mensagens_ia
    
    def extrair_telefones_unicos(df):
        """Extrai telefones únicos das mensagens"""
        if df.empty:
            return []
        
        if 'phoneNumber' not in df.columns:
            logger.error("❌ Campo 'phoneNumber' não encontrado")
            return []
        
        # Extrair telefones únicos
        telefones = df['phoneNumber'].dropna().unique()
        
        # Limpar telefones (remover vazios)
        telefones_limpos = []
        for tel in telefones:
            tel_str = str(tel).strip()
            if tel_str and tel_str != 'nan':
                telefones_limpos.append(tel_str)
        
        logger.info(f"📱 Telefones únicos: {len(telefones_limpos)}")
        
        return telefones_limpos
    
    # === EXECUÇÃO PRINCIPAL ===
    
    try:
        logger.info("🚀 Iniciando extração de telefones com IA")
        
        # 1. Buscar dados
        df_mensagens = buscar_tabela_baserow()
        if df_mensagens.empty:
            return {
                'sucesso': True,
                'total_telefones': 0,
                'telefones': [],
                'periodo': {
                    'data_hora_inicio': request.data_hora_inicio,
                    'data_hora_fim': request.data_hora_fim
                },
                'erro': 'Tabela vazia'
            }
        
        # 2. Filtrar por período
        df_periodo = filtrar_por_periodo(df_mensagens)
        if df_periodo.empty:
            return {
                'sucesso': True,
                'total_telefones': 0,
                'telefones': [],
                'periodo': {
                    'data_hora_inicio': request.data_hora_inicio,
                    'data_hora_fim': request.data_hora_fim
                },
                'erro': 'Nenhuma mensagem no período'
            }
        
        # 3. Identificar mensagens IA
        mensagens_ia = identificar_mensagens_ia(df_periodo)
        if mensagens_ia.empty:
            return {
                'sucesso': True,
                'total_telefones': 0,
                'telefones': [],
                'periodo': {
                    'data_hora_inicio': request.data_hora_inicio,
                    'data_hora_fim': request.data_hora_fim
                },
                'erro': 'Nenhuma mensagem de IA encontrada'
            }
        
        # 4. Extrair telefones únicos
        telefones_ia = extrair_telefones_unicos(mensagens_ia)
        
        logger.info(f"✅ RESULTADO: {len(telefones_ia)} telefones com IA")
        
        return {
            'sucesso': True,
            'total_telefones': len(telefones_ia),
            'telefones': telefones_ia,
            'periodo': {
                'data_hora_inicio': request.data_hora_inicio,
                'data_hora_fim': request.data_hora_fim
            },
            'erro': None
        }
    
    except Exception as e:
        error_msg = f"Erro: {str(e)}"
        logger.error(f"❌ {error_msg}")
        
        return {
            'sucesso': False,
            'total_telefones': 0,
            'telefones': [],
            'periodo': {
                'data_hora_inicio': request.data_hora_inicio,
                'data_hora_fim': request.data_hora_fim
            },
            'erro': error_msg
        }

@context_router.post("/extrair-telefones-conversas-periodo")
async def endpoint_extrair_telefones_conversas_periodo(request: TelefonesRequest):
    """
    🎯 ENDPOINT PARA EXTRAIR TELEFONES - Extrai lista de telefones que tiveram conversas no período
    """
    
    # === LOGGER LOCAL ===
    logger = logging.getLogger(__name__)
    
    
    
    # === VARIÁVEIS DE AMBIENTE ===
    
    baserow_token = os.getenv("BASEROW_API_TOKEN")
    baserow_graph_table_id = os.getenv("BASEROW_GRAPH_TABLE_ID")
    
    # Verificar se as variáveis obrigatórias estão definidas
    if not baserow_token:
        raise HTTPException(status_code=500, detail="BASEROW_API_TOKEN não definida no arquivo .env")
    if not baserow_graph_table_id:
        raise HTTPException(status_code=500, detail="BASEROW_GRAPH_TABLE_ID não definida no arquivo .env")
    
    # Converter ID para inteiro
    try:
        table_id = int(baserow_graph_table_id)
    except ValueError:
        raise HTTPException(status_code=500, detail="BASEROW_GRAPH_TABLE_ID deve ser um número inteiro")
    
    # === VALIDAÇÕES SIMPLES ===
    def validar_request():
        """Validações básicas do request"""
        # Validar formato de datas se fornecidas
        if request.data_hora_inicio is not None and request.data_hora_inicio.strip():
            try:
                datetime.strptime(request.data_hora_inicio, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise HTTPException(status_code=400, detail="data_hora_inicio formato: 'YYYY-MM-DD HH:MM:SS'")
        
        if request.data_hora_fim is not None and request.data_hora_fim.strip():
            try:
                datetime.strptime(request.data_hora_fim, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise HTTPException(status_code=400, detail="data_hora_fim formato: 'YYYY-MM-DD HH:MM:SS'")
    
    # Executar validações
    validar_request()
    
    # === FUNÇÕES AUXILIARES ===
    
    def get_table_as_dataframe(table_id, token):
        """
        Busca toda a tabela do Baserow e retorna como pandas DataFrame
        
        Args:
            table_id (int): ID da tabela
            token (str): Token de autenticação
        
        Returns:
            pandas.DataFrame: DataFrame com todos os dados da tabela
        """
        base_url = "https://baserow-baserow.hqgf38.easypanel.host"
        headers = {"Authorization": f"Token {token}"}
        
        all_records = []
        page = 1
        
        logger.info(f"Buscando dados da tabela {table_id}...")
        
        while True:
            url = f"{base_url}/api/database/rows/table/{table_id}/"
            params = {
                "user_field_names": "true",
                "size": 200,
                "page": page
            }
            
            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                results = data.get('results', [])
                
                if not results:
                    break
                    
                all_records.extend(results)
                logger.info(f"Página {page}: +{len(results)} registros")
                
                if not data.get('next'):
                    break
                    
                page += 1
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Erro: {e}")
                break
        
        # Converte para DataFrame
        if all_records:
            df = pd.DataFrame(all_records)
            logger.info(f"✅ DataFrame criado com {len(df)} linhas e {len(df.columns)} colunas")
            return df
        else:
            logger.info("❌ Nenhum dado encontrado")
            return pd.DataFrame()
    
    def _convert_brazilian_datetime_to_utc_timestamp(
        start_date: str, 
        start_time: str, 
        end_date: str, 
        end_time: str
    ) -> tuple:
        """
        Converte data/hora brasileira para timestamp UTC em milissegundos
        
        Args:
            start_date: Data inicial no formato 'dd/mm/yyyy'
            start_time: Hora inicial no formato 'HH:MM'
            end_date: Data final no formato 'dd/mm/yyyy'
            end_time: Hora final no formato 'HH:MM'
        
        Returns:
            tuple: (start_timestamp_ms, end_timestamp_ms)
        """
        br_tz = pytz.timezone('America/Sao_Paulo')
        utc_tz = pytz.UTC
        
        # Converte data/hora inicial
        start_datetime_str = f"{start_date} {start_time}"
        start_br = datetime.strptime(start_datetime_str, '%d/%m/%Y %H:%M')
        start_br_localized = br_tz.localize(start_br)
        start_utc = start_br_localized.astimezone(utc_tz)
        start_timestamp_ms = int(start_utc.timestamp() * 1000)
        
        # Converte data/hora final
        end_datetime_str = f"{end_date} {end_time}"
        end_br = datetime.strptime(end_datetime_str, '%d/%m/%Y %H:%M')
        end_br_localized = br_tz.localize(end_br)
        end_utc = end_br_localized.astimezone(utc_tz)
        end_timestamp_ms = int(end_utc.timestamp() * 1000)
        
        return start_timestamp_ms, end_timestamp_ms
    
    def convert_iso_to_brazilian_format(iso_datetime: str) -> tuple:
        """
        Converte formato ISO para formato brasileiro
        
        Args:
            iso_datetime: Data no formato 'YYYY-MM-DD HH:MM:SS'
        
        Returns:
            tuple: (data_br, hora_br) no formato ('dd/mm/yyyy', 'HH:MM')
        """
        dt = datetime.strptime(iso_datetime, '%Y-%m-%d %H:%M:%S')
        data_br = dt.strftime('%d/%m/%Y')
        hora_br = dt.strftime('%H:%M')
        return data_br, hora_br
    
    def new_filter_by_brazilian_datetime_or(
        df: pd.DataFrame, 
        start_date: str, 
        start_time: str, 
        end_date: str, 
        end_time: str,
        timestamp_fields: Union[str, List[str]] = ['start_timeTimestamp', 'last_updateTimestamp'],
        verbose: bool = True
    ) -> pd.DataFrame:
        """
        Filtra DataFrame por intervalo de data/hora brasileira usando lógica OR
        Retorna registros que tenham pelo menos um dos campos de timestamp no período especificado
        
        Args:
            df: DataFrame para filtrar
            start_date: Data inicial no formato 'dd/mm/yyyy'
            start_time: Hora inicial no formato 'HH:MM'
            end_date: Data final no formato 'dd/mm/yyyy'
            end_time: Hora final no formato 'HH:MM'
            timestamp_fields: Campo(s) de timestamp para filtrar. Pode ser:
                - str: nome de um campo único
                - List[str]: lista de campos para aplicar lógica OR
            verbose: Se True, imprime informações de debug
        
        Returns:
            DataFrame filtrado
            
        Raises:
            ValueError: Se os campos especificados não existirem no DataFrame
            ValueError: Se o formato de data/hora for inválido
        """
        
        # Normaliza timestamp_fields para sempre ser uma lista
        if isinstance(timestamp_fields, str):
            timestamp_fields = [timestamp_fields]
        
        # Valida se os campos existem no DataFrame
        missing_fields = [field for field in timestamp_fields if field not in df.columns]
        if missing_fields:
            raise ValueError(f"Campos não encontrados no DataFrame: {missing_fields}")
        
        # Cria cópia do DataFrame
        df_copy = df.copy()
        
        # Converte campos para numérico, forçando valores inválidos para NaN
        for field in timestamp_fields:
            df_copy[field] = pd.to_numeric(df_copy[field], errors='coerce')
            if verbose:
                logger.info(f"Campo '{field}' convertido para tipo: {df_copy[field].dtype}")
        
        # Converte datas brasileiras para timestamps UTC
        try:
            start_timestamp_ms, end_timestamp_ms = _convert_brazilian_datetime_to_utc_timestamp(
                start_date, start_time, end_date, end_time
            )
        except ValueError as e:
            raise ValueError(f"Erro na conversão de data/hora: {e}")
        
        if verbose:
            logger.info(f"Filtro brasileiro: {start_date} {start_time} até {end_date} {end_time}")
            logger.info(f"Convertido para UTC timestamp: {start_timestamp_ms} até {end_timestamp_ms}")
        
        # Aplica filtro OR para todos os campos especificados
        conditions = []
        field_counts = {}
        
        for field in timestamp_fields:
            condition = (
                (df_copy[field].notna()) &
                (df_copy[field] >= start_timestamp_ms) & 
                (df_copy[field] <= end_timestamp_ms)
            )
            conditions.append(condition)
            field_counts[field] = condition.sum()
            
            if verbose:
                logger.info(f"Registros com '{field}' no período: {field_counts[field]}")
        
        # Combina todas as condições com OR
        combined_condition = conditions[0]
        for condition in conditions[1:]:
            combined_condition = combined_condition | condition
        
        # Aplica filtro
        filtered_df = df_copy[combined_condition]
        
        if verbose:
            logger.info(f"Total de registros únicos (OR entre {len(timestamp_fields)} campos): {len(filtered_df)}")
            
            # Mostra estatísticas detalhadas se mais de um campo
            if len(timestamp_fields) > 1:
                logger.info(f"Campos utilizados: {', '.join(timestamp_fields)}")
        
        return filtered_df
    
    # === EXECUÇÃO PRINCIPAL ===
    
    try:
        logger.info("🚀 Iniciando extração de telefones por período")
        
        # === BUSCAR DADOS DA TABELA DE GRAFOS ===
        df_grafos = get_table_as_dataframe(table_id, baserow_token)
        
        if df_grafos.empty:
            return {
                'sucesso': True,
                'total_telefones': 0,
                'telefones': [],
                'periodo': {
                    'data_hora_inicio': request.data_hora_inicio,
                    'data_hora_fim': request.data_hora_fim
                },
                'erro': None
            }
        
        # === DEFINIR PERÍODO DE BUSCA ===
        if request.data_hora_inicio and request.data_hora_fim:
            # Converter formato ISO para brasileiro
            data_inicio, hora_inicio = convert_iso_to_brazilian_format(request.data_hora_inicio)
            data_fim, hora_fim = convert_iso_to_brazilian_format(request.data_hora_fim)
            
            logger.info(f"Período especificado: {data_inicio} {hora_inicio} até {data_fim} {hora_fim}")
            
            # === FILTRAR DADOS POR PERÍODO ===
            filtered_df_grafos = new_filter_by_brazilian_datetime_or(
                df_grafos, 
                data_inicio, 
                hora_inicio, 
                data_fim, 
                hora_fim, 
                ['start_timeTimestamp', 'last_updateTimestamp']
            )
        else:
            # Se não especificar período, retorna todos
            logger.info("Nenhum período especificado, retornando todos os telefones")
            filtered_df_grafos = df_grafos
        
        # === EXTRAIR TELEFONES ===
        if 'phoneNumber' not in filtered_df_grafos.columns:
            raise HTTPException(status_code=500, detail="Coluna 'phoneNumber' não encontrada na tabela de grafos")
        
        # Extrair telefones únicos e não nulos
        telefones_series = filtered_df_grafos['phoneNumber'].dropna().astype(str)
        telefones_unicos = telefones_series.unique().tolist()
        
        # Remover strings vazias
        telefones_limpos = [tel for tel in telefones_unicos if tel.strip()]
        
        logger.info(f"✅ {len(telefones_limpos)} telefones únicos encontrados no período")
        
        return {
            'sucesso': True,
            'total_telefones': len(telefones_limpos),
            'telefones': telefones_limpos,
            'periodo': {
                'data_hora_inicio': request.data_hora_inicio,
                'data_hora_fim': request.data_hora_fim
            },
            'erro': None
        }
    
    except Exception as e:
        error_msg = f"Erro na extração de telefones: {str(e)}"
        logger.error(f"❌ {error_msg}")
        
        return {
            'sucesso': False,
            'total_telefones': 0,
            'telefones': [],
            'periodo': {
                'data_hora_inicio': request.data_hora_inicio,
                'data_hora_fim': request.data_hora_fim
            },
            'erro': error_msg
        }


@context_router.post("/buscar-conversas-numero-com-fallback")
async def endpoint_buscar_conversas_numero_com_fallback(request: WhatsAppRequest):
    """
    🎯 ENDPOINT V2 - Pipeline WhatsApp com Fallback para Baserow
    
    Tenta Evolution API primeiro, se falhar ou retornar 0 mensagens,
    faz fallback para tabela de conversas do Baserow
    
    NOVO: Se forcar_evolution_apenas=True, busca APENAS na Evolution (sem fallback)
    """
    
    # === LOGGER LOCAL ===

    logger = logging.getLogger(__name__)
    
    
    
    # === VARIÁVEIS DE AMBIENTE ===
    
    evolution_url = os.getenv("EVOLUTION_URL")
    evolution_api_key = os.getenv("EVOLUTION_API_KEY") 
    evolution_instance_name = os.getenv("WHATSAPP_API_INSTANCE")
    agent_name = os.getenv("AGENT_NAME")
    baserow_token = os.getenv("BASEROW_API_TOKEN")
    baserow_message_table_id = os.getenv("BASEROW_MESSAGE_TABLE_ID")
    
    # Verificar variáveis obrigatórias
    if not evolution_url:
        raise HTTPException(status_code=500, detail="EVOLUTION_URL não definida no arquivo .env")
    if not evolution_api_key:
        raise HTTPException(status_code=500, detail="EVOLUTION_API_KEY não definida no arquivo .env")
    if not evolution_instance_name:
        raise HTTPException(status_code=500, detail="WHATSAPP_API_INSTANCE não definida no arquivo .env")
    if not agent_name:
        raise HTTPException(status_code=500, detail="AGENT_NAME não definida no arquivo .env")
    if not baserow_token:
        raise HTTPException(status_code=500, detail="BASEROW_API_TOKEN não definida no arquivo .env")
    if not baserow_message_table_id:
        raise HTTPException(status_code=500, detail="BASEROW_MESSAGE_TABLE_ID não definida no arquivo .env")
    
    # Converter ID para inteiro
    try:
        message_table_id = int(baserow_message_table_id)
    except ValueError:
        raise HTTPException(status_code=500, detail="BASEROW_MESSAGE_TABLE_ID deve ser um número inteiro")
    
    # === VALIDAÇÕES SIMPLES ===
    def validar_request():
        """Validações básicas do request"""
        # Validar horário comercial
        if len(request.horario_comercial) != 2:
            raise HTTPException(status_code=400, detail='horario_comercial deve ter 2 elementos [inicio, fim]')
        
        inicio, fim = request.horario_comercial
        if not (0 <= inicio <= 23 and 0 <= fim <= 23 and inicio < fim):
            raise HTTPException(status_code=400, detail='Horário comercial inválido')
        
        # Validar dias úteis
        if not all(0 <= dia <= 6 for dia in request.dias_uteis):
            raise HTTPException(status_code=400, detail='Dias úteis devem estar entre 0 e 6')
        
        # Validar datas se fornecidas
        if request.data_hora_inicio is not None and request.data_hora_inicio.strip():
            try:
                datetime.strptime(request.data_hora_inicio, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise HTTPException(status_code=400, detail="data_hora_inicio formato: 'YYYY-MM-DD HH:MM:SS'")
        
        if request.data_hora_fim is not None and request.data_hora_fim.strip():
            try:
                datetime.strptime(request.data_hora_fim, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise HTTPException(status_code=400, detail="data_hora_fim formato: 'YYYY-MM-DD HH:MM:SS'")
    
    # Executar validações
    validar_request()
    
    # === FUNÇÕES AUXILIARES (EVOLUTION) ===
    
    def converter_data_para_timestamp(data_str: str) -> int:
        """Converte string de data para timestamp"""
        try:
            dt = datetime.strptime(data_str, '%Y-%m-%d %H:%M:%S')
            return int(dt.timestamp())
        except ValueError:
            raise ValueError(f"Formato de data inválido: {data_str}")
    
    def filtrar_mensagens_por_periodo(mensagens: List[Dict]) -> List[Dict]:
        """Filtra mensagens pelo período especificado"""
        if not request.data_hora_inicio and not request.data_hora_fim:
            return mensagens
        
        mensagens_filtradas = []
        timestamp_inicio = None
        timestamp_fim = None
        
        if request.data_hora_inicio:
            timestamp_inicio = converter_data_para_timestamp(request.data_hora_inicio)
        
        if request.data_hora_fim:
            timestamp_fim = converter_data_para_timestamp(request.data_hora_fim)
        
        for msg in mensagens:
            timestamp_msg = msg.get('messageTimestamp', 0)
            
            if timestamp_inicio and timestamp_msg < timestamp_inicio:
                continue
                
            if timestamp_fim and timestamp_msg > timestamp_fim:
                continue
            
            mensagens_filtradas.append(msg)
        
        return mensagens_filtradas
    
    def buscar_nome_na_agenda(numero: str) -> str:
        """Busca nome do contato na agenda"""
        if not request.lista_contatos:
            return None
            
        def normalizar_numero(num: str) -> str:
            return ''.join(filter(str.isdigit, num))
        
        def extrair_numero_jid(jid: str) -> str:
            return jid.split('@')[0] if '@' in jid else jid
        
        def buscar_contato(numero_busca: str) -> str:
            numero_busca_norm = normalizar_numero(numero_busca)
            
            for contato in request.lista_contatos:
                numero_contato = None
                
                if 'remoteJid' in contato:
                    numero_contato = extrair_numero_jid(contato['remoteJid'])
                elif 'id' in contato:
                    numero_contato = extrair_numero_jid(contato['id'])
                
                if numero_contato:
                    numero_contato_norm = normalizar_numero(numero_contato)
                    
                    if numero_contato_norm == numero_busca_norm:
                        return contato.get('pushName') or contato.get('name') or contato.get('notify')
            return None
        
        numero_original = normalizar_numero(numero)
        
        # Tentativas de busca
        nome = buscar_contato(numero_original)
        if nome:
            return nome
        
        # Tentativas com variações brasileiras
        if numero_original.startswith('55'):
            if len(numero_original) == 13:
                numero_sem_9 = numero_original[:4] + numero_original[5:]
                nome = buscar_contato(numero_sem_9)
                if nome:
                    return nome
            
            elif len(numero_original) == 12:
                numero_com_9 = numero_original[:4] + '9' + numero_original[4:]
                nome = buscar_contato(numero_com_9)
                if nome:
                    return nome
            
            if len(numero_original) >= 10:
                numero_sem_pais = numero_original[2:]
                nome = buscar_contato(numero_sem_pais)
                if nome:
                    return nome
        
        return None
    
    def obter_nome_remetente(numero: str, push_name: str) -> str:
        """Obtém nome do remetente seguindo hierarquia de prioridade - VERSÃO CORRIGIDA"""
        
        # 1️⃣ PRIORIDADE 1: Nome salvo na agenda (busca inteligente)
        if request.lista_contatos:
            nome_salvo = buscar_nome_na_agenda(numero)
            if nome_salvo:
                return f"{nome_salvo} (contato salvo)"
        
        # 2️⃣ PRIORIDADE 2: pushName da mensagem
        if push_name and push_name != numero:
            # Se pushName é diferente do número, significa que o contato ESTÁ salvo no WhatsApp
            return f"{push_name} (contato salvo)"
        
        # 3️⃣ PRIORIDADE 3: Fallback - contato não salvo
        return f"({numero}) (remetente não salvo)"


    def extrair_conteudo_mensagem(msg: Dict[str, Any], remetente: str) -> str:
        """Extrai conteúdo da mensagem baseado no tipo"""
        message_type = msg.get('messageType', '')
        message_data = msg.get('message', {})
        
        if message_type in ['conversation', 'extendedTextMessage']:
            return message_data.get('conversation', '')
        
        elif message_type == 'documentMessage':
            doc_data = message_data.get('documentMessage', {})
            file_name = doc_data.get('fileName') or doc_data.get('title', 'arquivo')
            return f'[Documento "{file_name}" enviado por {remetente}]'
        
        elif message_type == 'audioMessage':
            audio_data = message_data.get('audioMessage', {})
            seconds = audio_data.get('seconds', 0)
            if seconds > 0:
                return f'[Áudio de {seconds} segundos enviado por {remetente}]'
            else:
                return f'[Mensagem de áudio enviada por {remetente}]'
        
        elif message_type == 'imageMessage':
            return f'[Imagem enviada por {remetente}]'
        
        elif message_type == 'videoMessage':
            return f'[Vídeo enviado por {remetente}]'
        
        elif message_type == 'stickerMessage':
            return f'[Sticker enviado por {remetente}]'
        
        elif message_type == 'contactMessage':
            contact_data = message_data.get('contactMessage', {})
            display_name = contact_data.get('displayName', '')
            if display_name:
                return f'[Contato "{display_name}" compartilhado por {remetente}]'
            else:
                return f'[Contato compartilhado por {remetente}]'
        
        else:
            return f'[Mensagem de tipo {message_type} enviada por {remetente}]'
    
    def calcular_tempo_comercial(timestamp_inicio: int, timestamp_fim: int) -> float:
        """Calcula tempo em horas considerando horário comercial"""
        inicio = datetime.fromtimestamp(timestamp_inicio)
        fim = datetime.fromtimestamp(timestamp_fim)
        
        if request.modo_quebra == 'simples':
            return (fim - inicio).total_seconds() / 3600
        
        horas_comerciais = 0
        current = inicio
        
        while current < fim:
            if current.weekday() in request.dias_uteis:
                hora = current.hour
                if request.horario_comercial[0] <= hora < request.horario_comercial[1]:
                    proxima_hora = min(current + timedelta(hours=1), fim)
                    if proxima_hora.hour <= request.horario_comercial[1] or proxima_hora.day > current.day:
                        horas_comerciais += (proxima_hora - current).total_seconds() / 3600
            
            current += timedelta(hours=1)
        
        return horas_comerciais
    
    def quebrar_mensagens_por_tempo(mensagens: List[Dict]) -> List[List[Dict]]:
        """Quebra mensagens em conversas por tempo"""
        if not mensagens:
            return []
        
        conversas = []
        conversa_atual = [mensagens[0]]
        
        for i in range(1, len(mensagens)):
            msg_anterior = mensagens[i-1]
            msg_atual = mensagens[i]
            
            tempo_decorrido = calcular_tempo_comercial(
                msg_anterior['timestamp'], 
                msg_atual['timestamp']
            )
            
            if tempo_decorrido > request.intervalo_max_horas:
                conversas.append(conversa_atual)
                conversa_atual = [msg_atual]
            else:
                conversa_atual.append(msg_atual)
        
        if conversa_atual:
            conversas.append(conversa_atual)
        
        return conversas
    
    # === FUNÇÕES AUXILIARES (BASEROW FALLBACK) ===
    
    def get_table_as_dataframe(table_id, token):
        """Busca tabela do Baserow como DataFrame"""
        base_url = "https://baserow-baserow.hqgf38.easypanel.host"
        headers = {"Authorization": f"Token {token}"}
        
        all_records = []
        page = 1
        
        logger.info(f"Buscando dados da tabela {table_id}...")
        
        while True:
            url = f"{base_url}/api/database/rows/table/{table_id}/"
            params = {
                "user_field_names": "true",
                "size": 200,
                "page": page
            }
            
            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                results = data.get('results', [])
                
                if not results:
                    break
                    
                all_records.extend(results)
                logger.info(f"Página {page}: +{len(results)} registros")
                
                if not data.get('next'):
                    break
                    
                page += 1
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Erro: {e}")
                break
        
        if all_records:
            df = pd.DataFrame(all_records)
            logger.info(f"✅ DataFrame criado com {len(df)} linhas e {len(df.columns)} colunas")
            return df
        else:
            logger.info("❌ Nenhum dado encontrado")
            return pd.DataFrame()
    
    def _convert_brazilian_datetime_to_utc_timestamp(
        start_date: str, 
        start_time: str, 
        end_date: str, 
        end_time: str
    ) -> tuple:
        """Converte data/hora brasileira para timestamp UTC em milissegundos"""
        br_tz = pytz.timezone('America/Sao_Paulo')
        utc_tz = pytz.UTC
        
        # Converte data/hora inicial
        start_datetime_str = f"{start_date} {start_time}"
        start_br = datetime.strptime(start_datetime_str, '%d/%m/%Y %H:%M')
        start_br_localized = br_tz.localize(start_br)
        start_utc = start_br_localized.astimezone(utc_tz)
        start_timestamp_ms = int(start_utc.timestamp() * 1000)
        
        # Converte data/hora final
        end_datetime_str = f"{end_date} {end_time}"
        end_br = datetime.strptime(end_datetime_str, '%d/%m/%Y %H:%M')
        end_br_localized = br_tz.localize(end_br)
        end_utc = end_br_localized.astimezone(utc_tz)
        end_timestamp_ms = int(end_utc.timestamp() * 1000)
        
        return start_timestamp_ms, end_timestamp_ms
    
    def convert_iso_to_brazilian_format(iso_datetime: str) -> tuple:
        """Converte formato ISO para brasileiro"""
        dt = datetime.strptime(iso_datetime, '%Y-%m-%d %H:%M:%S')
        data_br = dt.strftime('%d/%m/%Y')
        hora_br = dt.strftime('%H:%M')
        return data_br, hora_br
    
    def new_filter_by_brazilian_datetime_or(
        df: pd.DataFrame, 
        start_date: str, 
        start_time: str, 
        end_date: str, 
        end_time: str,
        timestamp_fields: Union[str, List[str]] = ['messageTimestamp'],
        verbose: bool = True
    ) -> pd.DataFrame:
        """Filtra DataFrame por intervalo de data/hora brasileira"""
        
        # Normaliza timestamp_fields para sempre ser uma lista
        if isinstance(timestamp_fields, str):
            timestamp_fields = [timestamp_fields]
        
        # Valida se os campos existem no DataFrame
        missing_fields = [field for field in timestamp_fields if field not in df.columns]
        if missing_fields:
            raise ValueError(f"Campos não encontrados no DataFrame: {missing_fields}")
        
        # Cria cópia do DataFrame
        df_copy = df.copy()
        
        # Converte campos para numérico
        for field in timestamp_fields:
            df_copy[field] = pd.to_numeric(df_copy[field], errors='coerce')
        
        # Converte datas brasileiras para timestamps UTC
        try:
            start_timestamp_ms, end_timestamp_ms = _convert_brazilian_datetime_to_utc_timestamp(
                start_date, start_time, end_date, end_time
            )
        except ValueError as e:
            raise ValueError(f"Erro na conversão de data/hora: {e}")
        
        # Aplica filtro OR para todos os campos especificados
        conditions = []
        
        for field in timestamp_fields:
            condition = (
                (df_copy[field].notna()) &
                (df_copy[field] >= start_timestamp_ms) & 
                (df_copy[field] <= end_timestamp_ms)
            )
            conditions.append(condition)
        
        # Combina todas as condições com OR
        combined_condition = conditions[0]
        for condition in conditions[1:]:
            combined_condition = combined_condition | condition
        
        # Aplica filtro
        filtered_df = df_copy[combined_condition]
        
        return filtered_df
    
    def extract_group_specific_conversation(df_banco, pushname_remetente, numero_conversa):
        """Extrai conversa específica do banco no formato da API WhatsApp"""
        
        # Filtrar mensagens pelo número específico
        conversas_filtradas = df_banco[
            df_banco['phoneNumber'] == numero_conversa
        ].copy()
        
        if conversas_filtradas.empty:
            logger.info(f"⚠️ Nenhuma mensagem encontrada para o número: {numero_conversa}")
            return []
        
        # Ordenar por timestamp
        conversas_filtradas = conversas_filtradas.sort_values('messageTimestamp')
        
        logger.info(f"📊 Encontradas {len(conversas_filtradas)} mensagens para {numero_conversa}")
        
        mensagens_formatadas = []
        
        for _, row in conversas_filtradas.iterrows():
            # Determinar se é fromMe
            from_me = row['pushName'] == pushname_remetente
            
            # Converter timestamp UTC milissegundos → fuso brasileiro segundos
            timestamp_utc_ms = row['messageTimestamp']
            
            # Garantir que é número
            if isinstance(timestamp_utc_ms, str):
                timestamp_utc_ms = int(float(timestamp_utc_ms))
            
            timestamp_utc_s = timestamp_utc_ms / 1000
            # Converter para fuso brasileiro (UTC-3)
            timestamp_br = int(timestamp_utc_s - (3 * 3600))
            
            # Criar estrutura da API
            mensagem_api = {
                'id': row['uuid'],
                'key': {
                    'id': row['uuid'][:20].upper().replace('-', ''),
                    'fromMe': from_me,
                    'remoteJid': f"{numero_conversa}@s.whatsapp.net"
                },
                'pushName': row['pushName'],
                'messageType': 'conversation',
                'message': {
                    'conversation': row['message']
                },
                'messageTimestamp': timestamp_br,
                'instanceId': row.get('instance', ''),
                'source': 'database'
            }
            
            mensagens_formatadas.append(mensagem_api)
        
        logger.info(f"✅ Conversa extraída com sucesso: {len(mensagens_formatadas)} mensagens formatadas")
        return mensagens_formatadas
     
    # === FUNÇÃO PRINCIPAL - TENTAR EVOLUTION ===
    
    async def tentar_evolution():
        """Tenta buscar mensagens da Evolution API"""
        
        logger.info("USANDO FUNÇÃO NOVA COM CONVERSOR")

        try:
            # NOVO: Converter número para formato Evolution automaticamente
            numero_original = request.numero_telefone
            
            def converter_numero_para_evolution(numero_input: str) -> str:
                """
                Converte número para formato Evolution API
                
                Entrada: 5531984036418 ou 553184036418 ou 31984036418
                Saída: 553184036418@s.whatsapp.net
                """
                # Remove caracteres não numéricos
                numero_limpo = ''.join(filter(str.isdigit, numero_input))
                
                # Se já tem @, retorna como está
                if '@' in numero_input:
                    return numero_input
                
                # Normalização para números brasileiros
                if len(numero_limpo) == 13 and numero_limpo.startswith('55'):
                    # 5531984036418 -> 553184036418 (remove o 9 extra)
                    numero_final = numero_limpo[:4] + numero_limpo[5:]
                elif len(numero_limpo) == 12 and numero_limpo.startswith('55'):
                    # 553184036418 -> mantém como está
                    numero_final = numero_limpo
                elif len(numero_limpo) == 11:
                    # 31984036418 -> 553184036418 (adiciona código do país)
                    numero_final = '55' + numero_limpo[2:]  # Remove primeiro 9 se for celular
                elif len(numero_limpo) == 10:
                    # 3184036418 -> 553184036418 (adiciona código do país + 9)
                    numero_final = '55' + numero_limpo
                else:
                    # Fallback: usa como está
                    numero_final = numero_limpo
                
                # Adiciona o sufixo do WhatsApp
                return f"{numero_final}@s.whatsapp.net"
            
            # Converter numero para formato Evolution
            numero_evolution = converter_numero_para_evolution(numero_original)
            
            logger.info(f"Número original: {numero_original}")
            logger.info(f"Número Evolution: {numero_evolution}")
            logger.info(f"🚀 Tentando Evolution para {numero_evolution}")
            
            # Buscar mensagens
            headers = {
                'Content-Type': 'application/json',
                'apikey': evolution_api_key
            }
            
            url = f"{evolution_url.rstrip('/')}/chat/findMessages/{evolution_instance_name}"
            todas_mensagens = []
            pagina_atual = 1
                        
            async with aiohttp.ClientSession() as session:
                while True:
                    logger.info(f"🔍 Buscando página {pagina_atual}")
                    
                    payload = {
                        "where": {
                            "key": {
                                "remoteJid": numero_evolution
                            }
                        },
                        "page": pagina_atual,
                        "offset": (pagina_atual - 1) * request.limite_por_pagina,
                        "limit": request.limite_por_pagina
                    }
                    
                    async with session.post(url, headers=headers, json=payload, timeout=request.timeout) as response:
                        response.raise_for_status()
                        resultado = await response.json()
                        
                        if isinstance(resultado, dict) and resultado.get('error'):
                            logger.error(f"❌ Erro da API: {resultado.get('message')}")
                            return None
                        
                        mensagens_pagina = []
                        
                        if isinstance(resultado, dict) and 'messages' in resultado:
                            info_mensagens = resultado['messages']
                            mensagens_pagina = info_mensagens.get('records', [])
                            total_paginas = info_mensagens.get('pages', 1)
                            
                            if pagina_atual == 1:
                                total_mensagens = info_mensagens.get('total', 0)
                                logger.info(f"📊 Total: {total_mensagens} mensagens")
                                logger.info(f"📄 Total páginas disponíveis: {total_paginas}")
                        
                        elif isinstance(resultado, list):
                            mensagens_pagina = resultado
                        
                        logger.info(f"📋 Página {pagina_atual}: {len(mensagens_pagina)} mensagens")
                        
                        # Para o loop se não há mais mensagens
                        if not mensagens_pagina:
                            logger.info("🛑 Parando: não há mais mensagens")
                            break
                        
                        todas_mensagens.extend(mensagens_pagina)
                        
                        pagina_atual += 1
                        
                        # Proteção contra loop infinito
                        if pagina_atual > 500:
                            logger.warning("Atingiu limite de 500 páginas por segurança")
                            break

            total_raw = len(todas_mensagens)
            
            # Filtrar por período
            mensagens_filtradas = filtrar_mensagens_por_periodo(todas_mensagens)
            total_filtradas = len(mensagens_filtradas)
            
            logger.info(f"✅ Evolution: {total_raw} raw, {total_filtradas} filtradas")
            
            # Retornar resultado
            return {
                'sucesso': True,
                'total_mensagens_raw': total_raw,
                'total_mensagens_filtradas': total_filtradas,
                'mensagens': mensagens_filtradas,
                'numero_original': numero_original,
                'numero_evolution_usado': numero_evolution  # Para debug
            }
            
        except Exception as e:
            logger.warning(f"⚠️ Evolution falhou: {str(e)}")
            return None

    # === FUNÇÃO FALLBACK - BASEROW ===
    
    def tentar_baserow():
        """Fallback: buscar mensagens do Baserow"""
        try:
            logger.info(f"🔄 Tentando fallback Baserow para {request.numero_telefone}")
            
            # Buscar tabela de conversas
            df_conversas = get_table_as_dataframe(message_table_id, baserow_token)
            
            if df_conversas.empty:
                logger.warning("❌ Tabela de conversas vazia")
                return None
            
            # Se tem filtro de período, aplicar
            if request.data_hora_inicio and request.data_hora_fim:
                data_inicio, hora_inicio = convert_iso_to_brazilian_format(request.data_hora_inicio)
                data_fim, hora_fim = convert_iso_to_brazilian_format(request.data_hora_fim)
                
                df_conversas = new_filter_by_brazilian_datetime_or(
                    df_conversas, 
                    data_inicio, 
                    hora_inicio, 
                    data_fim, 
                    hora_fim, 
                    ['messageTimestamp']
                )
            
            # Extrair conversa específica
            mensagens_formatadas = extract_group_specific_conversation(
                df_conversas, 
                agent_name, 
                request.numero_telefone
            )
            
            if not mensagens_formatadas:
                logger.warning(f"❌ Baserow: nenhuma mensagem para {request.numero_telefone}")
                return None
            
            logger.info(f"✅ Baserow: {len(mensagens_formatadas)} mensagens encontradas")
            
            return {
                'sucesso': True,
                'total_mensagens_raw': len(mensagens_formatadas),
                'total_mensagens_filtradas': len(mensagens_formatadas),
                'mensagens': mensagens_formatadas
            }
            
        except Exception as e:
            logger.error(f"❌ Baserow falhou: {str(e)}")
            return None
    
    # === EXECUÇÃO PRINCIPAL ===
    
    try:
        logger.info(f"🎯 V2: Processando conversa para {request.numero_telefone}")
        
        if request.forcar_evolution_apenas:
            logger.info("🚀 Modo Evolution APENAS ativado - sem fallback")
        
        # TENTATIVA 1: EVOLUTION
        resultado_evolution = await tentar_evolution()
        
        # Verificar se Evolution funcionou e tem mensagens
        if resultado_evolution and resultado_evolution['total_mensagens_filtradas'] > 0:
            logger.info("✅ Evolution: sucesso, processando mensagens...")
            mensagens_para_processar = resultado_evolution['mensagens']
            fonte = "evolution"
        elif request.forcar_evolution_apenas:
            # SE FORÇOU EVOLUTION APENAS E NÃO ENCONTROU, RETORNA ERRO/VAZIO
            logger.warning("❌ Evolution forçado não encontrou mensagens")
            return {
                'numero': request.numero_telefone,
                'sucesso': True,
                'total_mensagens_raw': 0,
                'total_mensagens_filtradas': 0,
                'conversa_processada': None,
                'fonte_dados': 'evolution_forcado',
                'erro': 'Nenhuma mensagem encontrada na Evolution (modo forçado)'
            }
        else:
            logger.info("🔄 Evolution falhou/0 mensagens, tentando Baserow...")
            
            # TENTATIVA 2: BASEROW (APENAS SE NÃO FORÇOU EVOLUTION)
            resultado_baserow = tentar_baserow()
            
            if resultado_baserow and resultado_baserow['total_mensagens_filtradas'] > 0:
                logger.info("✅ Baserow: sucesso, processando mensagens...")
                mensagens_para_processar = resultado_baserow['mensagens']
                fonte = "baserow"
            else:
                logger.warning("❌ Ambas as fontes falharam")
                return {
                    'numero': request.numero_telefone,
                    'sucesso': True,
                    'total_mensagens_raw': 0,
                    'total_mensagens_filtradas': 0,
                    'conversa_processada': None,
                    'fonte_dados': 'nenhuma',
                    'erro': 'Nenhuma mensagem encontrada em Evolution nem Baserow'
                }
        
        # PROCESSAR MENSAGENS (mesmo código do V1)
        mensagens_ordenadas = sorted(mensagens_para_processar, key=lambda x: x.get('messageTimestamp', 0))
        mensagens_estruturadas = []
        
        for msg in mensagens_ordenadas:
            timestamp = msg.get('messageTimestamp', 0)
            from_me = msg.get('key', {}).get('fromMe', False)
            message_type = msg.get('messageType', '')
            
            if from_me:
                push_name_atendente = msg.get('pushName', '')
                if push_name_atendente:
                    remetente = push_name_atendente
                else:
                    remetente = f"{agent_name} Agente IA"
            else:
                push_name = msg.get('pushName', '')
                remetente = obter_nome_remetente(request.numero_telefone, push_name)
            
            conteudo = extrair_conteudo_mensagem(msg, remetente)
            
            try:
                data_hora = datetime.fromtimestamp(timestamp).strftime('%d/%m/%Y %H:%M:%S')
            except (ValueError, OSError):
                data_hora = "Data inválida"
            
            mensagens_estruturadas.append({
                'timestamp': timestamp,
                'data_hora': data_hora,
                'remetente': remetente,
                'from_me': from_me,
                'message_type': message_type,
                'conteudo': conteudo,
                'push_name': msg.get('pushName', ''),
                'message_id': msg.get('key', {}).get('id', '')
            })
        
        # GERAR RESULTADO (mesmo código do V1)
        if not request.quebrar_conversas:
            # CONVERSA ÚNICA
            conversa_texto = f"=== CONVERSA COM NÚMERO: {request.numero_telefone} ===\n\n"
            conversa_objeto = []
            
            for msg in mensagens_estruturadas:
                conversa_texto += f"[{msg['data_hora']}] {msg['remetente']}: {msg['conteudo']}\n\n"
                
                conversa_objeto.append({
                    'data_hora_envio': msg['data_hora'],
                    'timestamp': msg['timestamp'],
                    'remetente': msg['remetente'],
                    'conteudo': msg['conteudo']
                })
            
            if request.formato_saida == 'texto':
                conversa_processada = {
                    'numero': request.numero_telefone,
                    'total_mensagens': len(mensagens_ordenadas),
                    'conversa_texto': conversa_texto,
                    'conversa_objeto': conversa_objeto
                }
            else:
                conversa_processada = {
                    'numero': request.numero_telefone,
                    'total_mensagens': len(mensagens_ordenadas),
                    'mensagens': mensagens_estruturadas
                }
            
            return {
                'numero': request.numero_telefone,
                'sucesso': True,
                'total_mensagens_raw': len(mensagens_para_processar),
                'total_mensagens_filtradas': len(mensagens_estruturadas),
                'conversa_processada': conversa_processada,
                'fonte_dados': fonte,  # 'evolution' ou 'baserow'
                'erro': None
            }
        
        else:
            # MÚLTIPLAS CONVERSAS
            conversas_quebradas = quebrar_mensagens_por_tempo(mensagens_estruturadas)
            lista_conversas = []
            
            for i, msgs_conversa in enumerate(conversas_quebradas):
                primeira_msg = msgs_conversa[0]
                ultima_msg = msgs_conversa[-1]
                periodo = f"{primeira_msg['data_hora']} - {ultima_msg['data_hora']}"
                
                if request.formato_saida == 'texto':
                    conversa_texto = f"=== CONVERSA {i+1} COM NÚMERO: {request.numero_telefone} ===\n"
                    conversa_texto += f"=== PERÍODO: {periodo} ===\n\n"
                    
                    conversa_objeto = []
                    
                    for msg in msgs_conversa:
                        conversa_texto += f"[{msg['data_hora']}] {msg['remetente']}: {msg['conteudo']}\n\n"
                        
                        conversa_objeto.append({
                            'data_hora_envio': msg['data_hora'],
                            'timestamp': msg['timestamp'],
                            'remetente': msg['remetente'],
                            'conteudo': msg['conteudo']
                        })
                    
                    lista_conversas.append({
                        'numero': request.numero_telefone,
                        'conversa_numero': i + 1,
                        'total_mensagens': len(msgs_conversa),
                        'periodo': periodo,
                        'conversa_texto': conversa_texto,
                        'conversa_objeto': conversa_objeto
                    })
                else:
                    lista_conversas.append({
                        'numero': request.numero_telefone,
                        'conversa_numero': i + 1,
                        'total_mensagens': len(msgs_conversa),
                        'periodo': periodo,
                        'mensagens': msgs_conversa
                    })
            
            return {
                'numero': request.numero_telefone,
                'sucesso': True,
                'total_mensagens_raw': len(mensagens_para_processar),
                'total_mensagens_filtradas': len(mensagens_estruturadas),
                'total_conversas': len(lista_conversas),
                'conversas': lista_conversas,
                'fonte_dados': fonte,  # 'evolution' ou 'baserow'
                'erro': None
            }
    
    except Exception as e:
        error_msg = f"Erro no processamento V2: {str(e)}"
        logger.error(f"❌ {error_msg}")
        
        return {
            'numero': request.numero_telefone,
            'sucesso': False,
            'total_mensagens_raw': 0,
            'total_mensagens_filtradas': 0,
            'conversa_processada': None,
            'fonte_dados': 'erro',
            'erro': error_msg
        }

@context_router.post("/analisar-conversa-atributos-quantitativos")
async def endpoint_analisar_conversa_atributos_quantitativos(request: AnaliseQuantitativaRequest):
    """
    📊 Analisa atributos quantitativos de uma conversa específica
    
    Simples: chama o endpoint que funciona + aplica a função que funciona
    """
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"🚀 Analisando conversa: {request.numero_telefone}")
        
        # === 1. CHAMAR ENDPOINT QUE JÁ FUNCIONA ===
        fallback_request = WhatsAppRequest(
            numero_telefone=request.numero_telefone,
            data_hora_inicio=request.data_hora_inicio,
            data_hora_fim=request.data_hora_fim,
            quebrar_conversas=False,
            formato_saida="texto"
        )
        
        resultado_fallback = await endpoint_buscar_conversas_numero_com_fallback(fallback_request)
        
        if not resultado_fallback.get('sucesso'):
            return {
                'sucesso': False,
                'numero_telefone': request.numero_telefone,
                'erro': f"Erro no fallback: {resultado_fallback.get('erro')}",
                'estatisticas': None
            }
        
        # === 2. PEGAR CONVERSA_OBJETO QUE JÁ ESTÁ PRONTO ===
        conversa_objeto = resultado_fallback['conversa_processada']['conversa_objeto']
        
        if not conversa_objeto:
            return {
                'sucesso': False,
                'numero_telefone': request.numero_telefone,
                'erro': 'conversa_objeto vazio',
                'estatisticas': None
            }
        
        # === 3. APLICAR SUA FUNÇÃO QUE JÁ FUNCIONA ===
        def analisar_conversas_atributos_quantitativos(conversa_objeto, chunk_size=2, teto_minutos=30):
            """Sua função original - copiada exatamente como está"""
            from functools import reduce
            
            def chunk_messages(messages, chunk_size):
                for i in range(0, len(messages), chunk_size):
                    yield messages[i:i + chunk_size]
            
            def detectar_remetentes(conversa_objeto):
                clientes = set()
                atendentes_humanos = set()
                atendente_ia = None
                
                for msg in conversa_objeto:
                    remetente = msg['remetente']
                    
                    if "Agente IA" in remetente:
                        atendente_ia = remetente
                    elif "(remetente não salvo)" in remetente or "(contato salvo)" in remetente:
                        clientes.add(remetente)
                    elif remetente.startswith("(") and remetente.endswith(")") and any(char.isdigit() for char in remetente):
                        clientes.add(remetente)
                    else:
                        if not any(pattern in remetente for pattern in ["553", "+55", "(remetente", "(contato"]):
                            atendentes_humanos.add(remetente)
                
                return {
                    'clientes': clientes,
                    'atendentes_humanos': atendentes_humanos,
                    'atendente_ia': atendente_ia
                }
            
            messages_chunks = list(chunk_messages(conversa_objeto, chunk_size))
            remetentes_detectados = detectar_remetentes(conversa_objeto)
            clientes = remetentes_detectados['clientes']
            atendentes_humanos = remetentes_detectados['atendentes_humanos']
            atendente_ia = remetentes_detectados['atendente_ia']
            
            def identificar_tipo_remetente(remetente: str) -> str:
                if atendente_ia and remetente == atendente_ia:
                    return 'atendente_ia'
                elif remetente in atendentes_humanos:
                    return 'atendente_humano'
                elif remetente in clientes:
                    return 'cliente'
                else:
                    if "Agente IA" in remetente:
                        return 'atendente_ia'
                    elif "(remetente não salvo)" in remetente or "(contato salvo)" in remetente:
                        return 'cliente'
                    else:
                        return 'atendente_humano'
            
            def conversation_reducer(acc, chunk):
                if len(chunk) < 2:
                    return acc
                
                first_msg = chunk[0]
                second_msg = chunk[1]
                first_sender = first_msg['remetente']
                second_sender = second_msg['remetente']
                
                first_type = identificar_tipo_remetente(first_sender)
                second_type = identificar_tipo_remetente(second_sender)
                
                if (first_type == 'cliente' and second_type in ['atendente_humano', 'atendente_ia']):
                    response_time = second_msg['timestamp'] - first_msg['timestamp']
                    acc['response_times'].append(response_time)
                    acc['total_response_time'] += response_time
                    
                    if second_type == 'atendente_humano':
                        acc['response_times_human'].append(response_time)
                        acc['total_response_time_human'] += response_time
                        
                        tempo_trabalho = calcular_tempo_trabalho_com_teto(response_time, teto_minutos)
                        acc['volume_tempo_trabalho'] += tempo_trabalho
                    else:
                        acc['response_times_ia'].append(response_time)
                        acc['total_response_time_ia'] += response_time
                
                for msg in chunk:
                    tipo_msg = identificar_tipo_remetente(msg['remetente'])
                    if tipo_msg == 'cliente':
                        dt = datetime.fromtimestamp(msg['timestamp'], tz=pytz.timezone('America/Sao_Paulo'))
                        hour = dt.hour
                        acc['client_activity_by_hour'][hour] = acc['client_activity_by_hour'].get(hour, 0) + 1
                        acc['total_client_messages'] += 1
                
                return acc
            
            def calcular_tempo_trabalho_com_teto(tempo_segundos: int, teto_minutos: int) -> int:
                teto_segundos = teto_minutos * 60
                if tempo_segundos <= teto_segundos:
                    return tempo_segundos
                else:
                    return 60
            
            initial_state = {
                'response_times': [],
                'total_response_time': 0,
                'response_times_human': [],
                'total_response_time_human': 0,
                'response_times_ia': [],
                'total_response_time_ia': 0,
                'client_activity_by_hour': {},
                'total_client_messages': 0,
                'volume_tempo_trabalho': 0
            }
            
            result = reduce(conversation_reducer, messages_chunks, initial_state)
            
            stats = {}
            stats['total_mensagens'] = len(conversa_objeto)
            stats['total_chunks'] = len(messages_chunks)
            stats['clientes_detectados'] = list(clientes)
            stats['atendentes_humanos_detectados'] = list(atendentes_humanos)
            stats['atendente_ia_detectado'] = atendente_ia
            stats['ia_presente'] = atendente_ia is not None
            
            if result['response_times']:
                stats['tempo_medio_resposta'] = result['total_response_time'] / len(result['response_times'])
            else:
                stats['tempo_medio_resposta'] = 0
            
            if result['response_times_human']:
                stats['tempo_medio_resposta_humano'] = result['total_response_time_human'] / len(result['response_times_human'])
            else:
                stats['tempo_medio_resposta_humano'] = 0
            
            if atendente_ia and result['response_times_ia']:
                stats['tempo_medio_resposta_ia'] = result['total_response_time_ia'] / len(result['response_times_ia'])
            else:
                stats['tempo_medio_resposta_ia'] = 0
            
            stats['volume_total_tempo_trabalho'] = result['volume_tempo_trabalho']
            
            if result['client_activity_by_hour']:
                melhor_hora = max(result['client_activity_by_hour'], key=result['client_activity_by_hour'].get)
                stats['melhor_horario_envio'] = melhor_hora
                stats['distribuicao_atividade_por_hora'] = result['client_activity_by_hour']
            else:
                stats['melhor_horario_envio'] = None
                stats['distribuicao_atividade_por_hora'] = {}
            
            if conversa_objeto:
                ultima_mensagem = conversa_objeto[-1]
                tipo_ultimo_remetente = identificar_tipo_remetente(ultima_mensagem['remetente'])
                
                if tipo_ultimo_remetente == 'cliente':
                    stats['ultimo_remetente'] = "Cliente"
                else:
                    stats['ultimo_remetente'] = "Atendimento"
                
                stats['data_hora_ultima_mensagem'] = datetime.fromtimestamp(ultima_mensagem['timestamp'], tz=pytz.timezone('America/Sao_Paulo'))
                
                ultima_mensagem_cliente = None
                for msg in reversed(conversa_objeto):
                    if identificar_tipo_remetente(msg['remetente']) == 'cliente':
                        ultima_mensagem_cliente = msg
                        break
                
                if ultima_mensagem_cliente:
                    stats['data_hora_ultima_mensagem_cliente'] = datetime.fromtimestamp(ultima_mensagem_cliente['timestamp'], tz=pytz.timezone('America/Sao_Paulo'))
                else:
                    stats['data_hora_ultima_mensagem_cliente'] = None
                
                ultima_mensagem_atendimento = None
                for msg in reversed(conversa_objeto):
                    tipo_msg = identificar_tipo_remetente(msg['remetente'])
                    if tipo_msg in ['atendente_humano', 'atendente_ia']:
                        ultima_mensagem_atendimento = msg
                        break
                
                if ultima_mensagem_atendimento:
                    stats['data_hora_ultima_mensagem_atendimento'] = datetime.fromtimestamp(ultima_mensagem_atendimento['timestamp'], tz=pytz.timezone('America/Sao_Paulo'))
                else:
                    stats['data_hora_ultima_mensagem_atendimento'] = None
            else:
                stats['ultimo_remetente'] = None
                stats['data_hora_ultima_mensagem'] = None
                stats['data_hora_ultima_mensagem_cliente'] = None
                stats['data_hora_ultima_mensagem_atendimento'] = None
            
            return stats
        
        # === 4. EXECUTAR ANÁLISE ===
        estatisticas = analisar_conversas_atributos_quantitativos(
            conversa_objeto, 
            chunk_size=2, 
            teto_minutos=request.teto_minutos
        )
        
        logger.info(f"✅ Análise concluída: {estatisticas['total_mensagens']} mensagens")
        
        # === 5. RETORNAR ===
        return {
            'sucesso': True,
            'numero_telefone': request.numero_telefone,
            'periodo': {
                'data_hora_inicio': request.data_hora_inicio,
                'data_hora_fim': request.data_hora_fim
            },
            'teto_minutos_usado': request.teto_minutos,
            'estatisticas': estatisticas,
            'erro': None
        }
    
    except Exception as e:
        error_msg = f"Erro na análise: {str(e)}"
        logger.error(f"❌ {error_msg}")
        
        return {
            'sucesso': False,
            'numero_telefone': request.numero_telefone,
            'erro': error_msg,
            'estatisticas': None
        }

@context_router.post("/analisar-conversa-atributos-qualitativos")
async def endpoint_analisar_conversa_atributos_qualitativos(request: AnaliseQualitativaRequest):
    """
    📊 Analisa atributos qualitativos de uma conversa específica usando LLM
    
    Contexto de negócio vem do .env automaticamente (pode ser sobrescrito no request)
    """
    
    logger = logging.getLogger(__name__)
    
    # === CONFIGURAÇÕES LLM MARITACA ===
    maritaca_api_key = os.getenv("MARITACA_API_KEY")
    
    # Configurações fixas
    maritaca_base_url = "https://chat.maritaca.ai/api"
    modelo_padrao = "sabia-3.1"
    
    if not maritaca_api_key:
        raise HTTPException(status_code=500, detail="MARITACA_API_KEY não definida no arquivo .env")
    
    # Usar modelo do request ou padrão hardcoded
    modelo_usar = request.modelo or modelo_padrao
    
    # === CONTEXTO DE NEGÓCIO (AUTOMATICO DO .ENV) ===
    contexto_negocio = request.contexto_negocio or ContextoNegocio()
    
    # Converter para dict para compatibilidade
    contexto_dict = {
        "nome_empresa": contexto_negocio.nome_empresa,
        "tipo_negocio": contexto_negocio.tipo_negocio,
        "especialidade": contexto_negocio.especialidade,
        "profissionais_principais": contexto_negocio.profissionais_principais,
        "cidade": contexto_negocio.cidade,
        "responsavel_atendimento": contexto_negocio.responsavel_atendimento,
        "numero_whatsapp": contexto_negocio.numero_whatsapp,
        "descricao_servicos": contexto_negocio.descricao_servicos
    }
    
    try:
        logger.info(f"🚀 Análise qualitativa: {request.numero_telefone}")
        logger.info(f"🤖 Modelo: {modelo_usar} | Temp: {request.temperatura}")
        logger.info(f"🏢 Empresa: {contexto_negocio.nome_empresa}")
        
        # === 1. BUSCAR CONVERSA ===
        fallback_request = WhatsAppRequest(
            numero_telefone=request.numero_telefone,
            data_hora_inicio=request.data_hora_inicio,
            data_hora_fim=request.data_hora_fim,
            quebrar_conversas=False,
            formato_saida="texto"
        )
        
        resultado_fallback = await endpoint_buscar_conversas_numero_com_fallback(fallback_request)
        
        if not resultado_fallback.get('sucesso'):
            return {
                'sucesso': False,
                'numero_telefone': request.numero_telefone,
                'erro': f"Erro ao buscar conversa: {resultado_fallback.get('erro')}",
                'analise': None
            }
        
        # === 2. EXTRAIR CONVERSA_OBJETO ===
        conversa_objeto = resultado_fallback['conversa_processada']['conversa_objeto']
        
        if not conversa_objeto:
            return {
                'sucesso': False,
                'numero_telefone': request.numero_telefone,
                'erro': 'conversa_objeto vazio',
                'analise': None
            }
        
        logger.info(f"✅ Conversa obtida: {len(conversa_objeto)} mensagens")
        
        # === 3. CONFIGURAR CLIENT MARITACA ===
        import openai
        
        client = openai.OpenAI(
            api_key=maritaca_api_key,
            base_url=maritaca_base_url,
        )
        
        # === 4. FUNÇÃO DE ANÁLISE QUALITATIVA (INNER FUNCTION) ===
        
        def analisar_conversas_atributos_qualitativos(
            conversa_objeto: list,
            contexto_negocio: dict,
            client,
            campos_analise: list = None,
            modelo: str = "sabia-3.1",
            temperatura: float = 0.1
        ):
            """Função de análise qualitativa completa"""
            
            # Campos padrão se não especificado
            if campos_analise is None:
                campos_analise = [
                    "identificacao", "engajamento", "comportamental", 
                    "historico_relacionamento", "validacao_cliente", "comercial",
                    "analise_avancada", "scores", "qualidade_dados", "status_operacional"
                ]
            
            # Converter conversa_objeto para texto formatado
            def formatar_conversa_para_llm(conversa_objeto):
                conversa_texto = ""
                for msg in conversa_objeto:
                    data_hora = msg.get('data_hora_envio', 'Data não disponível')
                    remetente = msg.get('remetente', 'Remetente desconhecido')
                    conteudo = msg.get('conteudo', '')
                    conversa_texto += f"[{data_hora}] {remetente}: {conteudo}\n\n"
                return conversa_texto
            
            # Construir seções do prompt baseado nos campos solicitados
            def construir_instrucoes_campos(campos_analise, contexto_negocio):
                instrucoes = ""
                
                if "identificacao" in campos_analise:
                    instrucoes += """
                📊 IDENTIFICAÇÃO E CONTEXTO:
                - numero_whatsapp: Número do WhatsApp analisado
                - nome_extraido: Nome mencionado nas conversas (paciente ou quem agenda)
                - tipo_interlocutor: Paciente direto | Familiar/acompanhante | Fornecedor/parceiro | Não identificado 
                - data_hora_primeira_mensagem: Data e hora da primeira mensagem
                - data_hora_ultima_mensagem: Data e hora da última mensagem 
                - primeira_mensagem: Quem enviou (lead/prospect/cliente | atendente)
                - ultima_mensagem: Quem enviou (lead/prospect/cliente | atendente)
                - resumo_conversa: 2-3 frases resumindo a conversa
                - cidade_origem: Cidade mencionada pelo cliente ou "Não identificado"
                """

                if "engajamento" in campos_analise:
                    instrucoes += """
                🔄 ENGAJAMENTO NO WHATSAPP:
                - total_mensagens_conversa: Número total de mensagens
                - proporcao_mensagens_cliente: % de mensagens enviadas pelo cliente
                - frequencia_interacao: Diária | Semanal | Mensal | Esporádica
                - padrao_resposta_cliente: Sempre responde | Responde seletivamente | Raramente responde | Nunca responde
                - tempo_primeira_resposta_cliente: Tempo para primeira resposta (segundos)
                - tempo_medio_resposta_cliente: Tempo médio de resposta (segundos)
                - tempo_maximo_sem_resposta: Maior gap sem resposta (segundos)
                """

                if "comportamental" in campos_analise:
                    instrucoes += """
                🎯 COMPORTAMENTAL NO CANAL:
                - tom_predominante_cliente: Colaborativo | Formal/educado | Ansioso/preocupado | Investigativo/questionador | Impaciente/exigente | Neutro/objetivo
                - evolucao_tom_conversa: Melhorou | Manteve-se estável | Piorou | Oscilante
                - nivel_proatividade: Alto (inicia conversas) | Médio (responde e pergunta) | Baixo (apenas responde)
                - sinais_satisfacao_expressos: Lista de indicadores positivos
                - sinais_insatisfacao_expressos: Lista de indicadores negativos
                - resultado_final_conversa: Agendamento consulta concluído | Agendamento consulta em andamento | Agendamento cirurgia concluído | Agendamento cirurgia em andamento | Agendamento de Procedimento/Tratamento concluído | Agendamento de Procedimento/Tratamento em andamento | Múltiplos | Não conclusivo
                """

                if "historico_relacionamento" in campos_analise:
                    instrucoes += """
                🏥 HISTÓRICO DE RELACIONAMENTO:
                - referencias_interacoes_anteriores: Menciona específicas | Fala genericamente | Primeira vez | Não fica claro
                - servicos_mencionados: Lista de serviços/produtos citados
                - profissionais_citados: Nomes de profissionais/atendentes mencionados
                - tempo_ultima_interacao_mencionada: Quando cliente menciona última interação
                - continuidade_relacionamento: Acompanhamento | Retorno pontual | Novo relacionamento | Urgência
                """

                if "validacao_cliente" in campos_analise:
                    instrucoes += """
                🩺 VALIDAÇÃO STATUS CLIENTE:
                - status_cliente_whatsapp: Cliente confirmado | Provável cliente | Não é cliente | Inconclusivo
                - evidencias_cliente_existente: Menções explícitas de interações/compras anteriores
                - evidencias_cliente_novo: Indicadores claros de primeira vez
                - incertezas_historico: Gaps ou ambiguidades sobre histórico de relacionamento
                - tipo_demanda_mencionada: Baseado no tipo de negócio (ex: Consulta | Compra | Suporte | Informação | Múltiplos | Não especificado)
                """

                if "comercial" in campos_analise:
                    instrucoes += """
                💰 ASPECTOS COMERCIAIS:
                - sensibilidade_preco_expressa: Alta | Média | Baixa | Não mencionado
                - mencoes_convenio: Planos citados ou perguntas sobre cobertura
                - poder_decisao_aparente: Decide sozinho | Consulta terceiros | Agenda para outros | Não fica claro
                - urgencia_expressa: Muito urgente | Urgente | Moderada | Flexível
                """

                if "analise_avancada" in campos_analise:
                    instrucoes += """
                🧠 ANÁLISE AVANÇADA:
                - evolucao_interesse: Cresceu | Manteve | Diminuiu | Oscilante
                - perfil_comunicacao: Ansioso | Prático | Detalhista | Desconfiado | Colaborativo | Impaciente
                - preocupacoes_implicitas: Hesitações não verbalizadas
                - motivacoes_nao_verbalizadas: O que realmente motiva além do declarado
                - objecoes_nao_expressas: Barreiras percebidas mas não mencionadas
                """

                if "scores" in campos_analise:
                    instrucoes += """
                🎯 SCORES (1-10):
                - engagement_score: baseado em frequência + qualidade + proatividade
                - conversion_readiness_score: baseado em urgência + ausência objeções + poder decisão
                - loyalty_potential_score: baseado em satisfação + recorrência + sinais fidelização
                - risk_score: baseado em insatisfação + gaps + mudanças negativas
                """

                if "qualidade_dados" in campos_analise:
                    instrucoes += """
                ⚠️ QUALIDADE DOS DADOS:
                - nivel_certeza_analise: Alta | Média | Baixa | Inconclusiva
                - necessita_validacao_externa: Sim | Não
                - base_analise: Descrição da fonte dos dados
                - confiabilidade_dados: Percentual estimado de completude
                """

                if "status_operacional" in campos_analise:
                    instrucoes += """
                🚨 STATUS OPERACIONAL:
                - status_conversa: Concluída | Inconclusivo | Pendente com necessidade de ações lead/prospect/cliente | Pendente com necessidade de ações atendente
                - acao_imediata_necessaria: Sim | Não | Inconclusivo
                - proximo_passo: Próximo passo necessário baseado na conversa
                """

                if "avaliacao_agente_ia" in campos_analise:
                    instrucoes += """
                🤖 AVALIAÇÃO AGENTE IA:
                
                📊 IDENTIFICAÇÃO E DETECÇÃO IA:
                - momentos_ativacao_ia: Quando a IA entrou na conversa
                - transicoes_ia_humano: Quantas vezes passou de IA → humano
                
                🎯 EFICÁCIA NA CONDUÇÃO:
                - intencao_cliente_identificada: Corretamente | Parcialmente | Incorretamente | Não identificou
                - direcionamento_conversa: Muito eficaz | Eficaz | Pouco eficaz | Ineficaz
                - conclusao_demanda_ia: Resolveu completamente | Resolveu parcialmente | Não resolveu | Piorou situação
                - escalacoes_necessarias: Nenhuma | 1 escalação | Múltiplas escalações | Falha na escalação
                - loops_conversa: Nenhum | Poucos | Muitos | Loop infinito
                
                💬 QUALIDADE DAS RESPOSTAS:
                - precisao_informacoes: Todas corretas | Maioria corretas | Algumas incorretas | Maioria incorretas
                - completude_respostas: Sempre completas | Geralmente completas | Parciais | Incompletas
                - relevancia_contexto: Sempre relevante | Geralmente relevante | Às vezes irrelevante | Frequentemente irrelevante
                - personalizacao_atendimento: Alta | Média | Baixa | Nenhuma
                - tom_comunicacao_ia: Muito apropriado | Apropriado | Pouco apropriado | Inapropriado
                
                ⚡ EFICIÊNCIA OPERACIONAL:
                - resolucoes_primeiro_contato: Sim | Parcial | Não | Agravou
                - necessidade_repeticoes: Nenhuma | Poucas | Muitas | Excessivas
                - handoff_qualidade: Muito boa | Boa | Ruim | Muito ruim | Não houve
                - continuidade_contexto: Perfeita | Boa | Parcial | Perdida
                
                😤 FRUSTRAÇÃO E LIMITAÇÕES:
                - sinais_frustracao_ia: Nenhum | Leves | Moderados | Altos
                - tentativas_burlar_ia: Nenhuma | Poucas | Muitas | Persistentes
                - solicitacoes_humano: Nenhuma | Eventual | Frequente | Constante
                - limitacoes_evidentes: Lista de limitações identificadas
                - momentos_criticos_falha: Quando a IA falhou criticamente
                
                🔄 APRENDIZADO E ADAPTAÇÃO:
                - correcoes_durante_conversa: Sim | Não | Parcialmente
                - melhoria_ao_longo_conversa: Sim | Não | Manteve padrão
                - uso_historico_cliente: Muito bem | Bem | Pouco | Não usou
                - adaptacao_linguagem_cliente: Muito boa | Boa | Pouca | Nenhuma
                
                📈 IMPACTO NO RESULTADO:
                - contribuicao_resultado_final: Muito positiva | Positiva | Neutra | Negativa | Muito negativa
                - satisfacao_cliente_com_ia: Muito satisfeito | Satisfeito | Neutro | Insatisfeito | Muito insatisfeito
                - preferencia_expressa: Prefere IA | Indiferente | Prefere humano | Rejeita IA
                - economia_tempo_gerada: Alta | Média | Baixa | Nenhuma | Perdeu tempo
                
                🎯 SCORES IA (1-10):
                - score_compreensao_intent: Capacidade de entender intenção
                - score_qualidade_respostas: Precisão e completude
                - score_eficiencia_processo: Velocidade e resolução
                - score_experiencia_cliente: Satisfação geral com IA
                - score_handoff_humano: Qualidade da transição
                
                🔧 OPORTUNIDADES DE MELHORIA:
                - gaps_conhecimento_identificados: Lacunas no conhecimento da IA
                - padroes_falha_recorrentes: Padrões de erro identificados
                - sugestoes_treinamento: Áreas para melhorar o treinamento
                - ajustes_fluxo_sugeridos: Mudanças no fluxo de conversa
                - momentos_ideal_escalacao: Quando deveria ter escalado
                """

                if "status_atendimento_conversa" in campos_analise:
                    instrucoes += """
                📋 STATUS ATENDIMENTO CONVERSA:
                - conversa_em_aberto: Sim | Não
                - pendencia_com_quem: Cliente | Atendente | Ambos | Nenhuma pendência
                - ultima_pendencia_identificada: Descrição específica da última pendência identificada na conversa
                - resumo_interacoes_ate_o_momento: Resume todas as conversas dando um status do momento atual
                """

                # === NOVA CATEGORIA: MEMÓRIA CONTEXTO ===
                if "memoria_contexto" in campos_analise:
                    instrucoes += """
                🧠 MEMÓRIA CONTEXTO:
                - resumo_interacoes_ate_o_momento: Resume todas as conversas dando um status do momento atual
                - tom_predominante_cliente: Colaborativo | Formal/educado | Ansioso/preocupado | Investigativo/questionador | Impaciente/exigente | Neutro/objetivo
                - status_cliente_whatsapp: Cliente confirmado | Provável cliente | Não é cliente | Inconclusivo
                - conversa_em_aberto: Sim | Não
                - pendencia_com_quem: Cliente | Atendente | Ambos | Nenhuma pendência
                """
                
                return instrucoes

            try:
                # Formatar conversa
                conversa_texto = formatar_conversa_para_llm(conversa_objeto)
                
                # Construir contexto dinâmico
                contexto_dinamico = f"""
                CONTEXTO DO NEGÓCIO:
                - {contexto_negocio.get('nome_empresa', 'Empresa')} é uma {contexto_negocio.get('tipo_negocio', 'empresa')} especializada em {contexto_negocio.get('especialidade', 'serviços diversos')}.
                - Profissionais/Responsáveis principais: {', '.join(contexto_negocio.get('profissionais_principais', ['Não especificado']))}
                - Localização: {contexto_negocio.get('cidade', 'Não especificado')}
                - Atendimento WhatsApp: {contexto_negocio.get('responsavel_atendimento', 'Atendente')} no número {contexto_negocio.get('numero_whatsapp', 'Não especificado')}
                - Serviços: {contexto_negocio.get('descricao_servicos', 'Serviços diversos')}
                """
                
                # Construir instruções baseadas nos campos solicitados
                instrucoes_campos = construir_instrucoes_campos(campos_analise, contexto_negocio)
                
                prompt_sistema = f"""
                Você é um especialista em análise de relacionamento cliente-empresa. Sua função é analisar conversas de WhatsApp entre clientes/prospects e empresas.

                {contexto_dinamico}

                DIRETRIZES GERAIS:
                - Análise baseada EXCLUSIVAMENTE nas mensagens WhatsApp fornecidas
                - NÃO faça suposições sobre atividades fora do canal WhatsApp
                - Sinalize quando dados são inconclusivos ou podem existir atividades em outros canais
                - Use "Não identificado" quando a informação não estiver disponível
                - Use "Inconclusivo" quando os dados forem contraditórios ou insuficientes

                INSTRUÇÕES DETALHADAS POR CATEGORIA:
                {instrucoes_campos}

                FORMATO DE RESPOSTA:
                Retorne os dados em formato JSON estruturado, com todos os campos solicitados preenchidos.
                """
                
                prompt_usuario = f"""
                Analise esta conversa de WhatsApp:
                
                {conversa_texto}
                
                Extraia os atributos solicitados, focando em:
                1. Evolução do comportamento ao longo da conversa
                2. Sinais implícitos de satisfação/insatisfação  
                3. Oportunidades identificadas
                4. Padrões de comunicação e engajamento
                5. Precisão sobre limitações dos dados (apenas WhatsApp)
                """
                
                # Fazer chamada para o LLM
                completion = client.beta.chat.completions.parse(
                    model=modelo,
                    messages=[
                        {"role": "system", "content": prompt_sistema},
                        {"role": "user", "content": prompt_usuario},
                    ],
                    response_format={"type": "json_object"},
                    temperature=temperatura
                )
                
                # Retornar resultado parseado
                resultado_json = completion.choices[0].message.content
                
                # Tentar parsear JSON
                import json
                try:
                    resultado_parseado = json.loads(resultado_json)
                    return {
                        "sucesso": True,
                        "dados": resultado_parseado,
                        "campos_analisados": campos_analise,
                        "contexto_usado": contexto_negocio,
                        "total_mensagens": len(conversa_objeto)
                    }
                except json.JSONDecodeError as e:
                    return {
                        "sucesso": False,
                        "erro": f"Erro ao parsear JSON: {str(e)}",
                        "resposta_bruta": resultado_json
                    }
                
            except Exception as e:
                return {
                    "sucesso": False,
                    "erro": f"Erro ao analisar conversa: {str(e)}",
                    "campos_analisados": campos_analise,
                    "contexto_usado": contexto_negocio
                }
        
        # === 5. EXECUTAR ANÁLISE QUALITATIVA ===
        resultado_analise = analisar_conversas_atributos_qualitativos(
            conversa_objeto=conversa_objeto,
            contexto_negocio=contexto_dict,
            client=client,
            campos_analise=request.campos_analise,
            modelo=modelo_usar,
            temperatura=request.temperatura
        )
        
        logger.info(f"✅ Análise qualitativa concluída: {resultado_analise.get('sucesso')}")
        
        # === 6. RETORNAR ===
        return {
            'sucesso': True,
            'numero_telefone': request.numero_telefone,
            'periodo': {
                'data_hora_inicio': request.data_hora_inicio,
                'data_hora_fim': request.data_hora_fim
            },
            'parametros_llm': {
                'modelo': modelo_usar,
                'temperatura': request.temperatura,
                'campos_analisados': request.campos_analise or "padrão",
                'provider': "Maritaca AI"
            },
            'contexto_usado': contexto_dict,
            'analise': resultado_analise,
            'erro': None
        }
    
    except Exception as e:
        error_msg = f"Erro na análise qualitativa: {str(e)}"
        logger.error(f"❌ {error_msg}")
        
        return {
            'sucesso': False,
            'numero_telefone': request.numero_telefone,
            'erro': error_msg,
            'analise': None
        }

@context_router.post("/obter-memoria-contexto")
async def endpoint_obter_memoria_contexto(request: AnaliseCompletaRequest):
    """
    🧠 Obter Memória Contexto - Fusão inteligente de dados quantitativos + qualitativos
    
    Executa análises quantitativa e qualitativa em paralelo usando asyncio.gather() 
    e retorna uma fusão focada com insights correlacionados
    """
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"🚀 Obter memória contexto iniciado: {request.numero_telefone}")
        
        # === PREPARAR REQUESTS PARA AS ANÁLISES ===
        
        # Request quantitativa
        request_quanti = AnaliseQuantitativaRequest(
            numero_telefone=request.numero_telefone,
            data_hora_inicio=request.data_hora_inicio,
            data_hora_fim=request.data_hora_fim,
            teto_minutos=request.teto_minutos or 30
        )
        
        # Request qualitativa (APENAS memoria_contexto para fusão focada)
        campos_quali = ["memoria_contexto"]
        
        request_quali = AnaliseQualitativaRequest(
            numero_telefone=request.numero_telefone,
            data_hora_inicio=request.data_hora_inicio,
            data_hora_fim=request.data_hora_fim,
            campos_analise=campos_quali,
            temperatura=request.temperatura or 0.1,
            modelo=request.modelo
        )
        
        # === EXECUÇÃO PARALELA COM ASYNCIO.GATHER ===
        
        logger.info("⚡ Executando análises em paralelo...")
        
        # Criar tasks
        task_quanti = asyncio.create_task(
            endpoint_analisar_conversa_atributos_quantitativos(request_quanti)
        )
        
        task_quali = asyncio.create_task(
            endpoint_analisar_conversa_atributos_qualitativos(request_quali)
        )
        
        # Executar em paralelo - return_exceptions=True para capturar erros
        resultado_quanti, resultado_quali = await asyncio.gather(
            task_quanti, task_quali, return_exceptions=True
        )
        
        # === VERIFICAR RESULTADOS ===
        
        # Verificar se houve exceções
        erro_quanti = isinstance(resultado_quanti, Exception)
        erro_quali = isinstance(resultado_quali, Exception)
        
        if erro_quanti and erro_quali:
            return {
                'sucesso': False,
                'numero_telefone': request.numero_telefone,
                'erro': 'Ambas as análises falharam',
                'erro_quantitativa': str(resultado_quanti),
                'erro_qualitativa': str(resultado_quali),
                'analise_completa': None
            }
        
        # === FUSÃO INTELIGENTE DOS RESULTADOS ===
        
        def fusionar_analises(quanti_result, quali_result):
            """Fusiona resultados das análises quantitativa e qualitativa"""
            
            fusao = {
                'status_execucao': {
                    'quantitativa_sucesso': not erro_quanti and quanti_result.get('sucesso', False),
                    'qualitativa_sucesso': not erro_quali and quali_result.get('sucesso', False),
                    'timestamp_analise': datetime.now().isoformat()
                },
                'dados_quantitativos': {},
                'dados_qualitativos': {}
            }
            
            # === EXTRAIR DADOS QUANTITATIVOS (APENAS CAMPOS ESSENCIAIS) ===
            if not erro_quanti and quanti_result.get('sucesso'):
                stats = quanti_result.get('estatisticas', {})
                fusao['dados_quantitativos'] = {
                    'ultimo_remetente': stats.get('ultimo_remetente'),
                    'data_ultima_mensagem_cliente': stats.get('data_hora_ultima_mensagem_cliente'),
                    'data_ultima_mensagem_atendimento': stats.get('data_hora_ultima_mensagem_atendimento'),
                    'melhor_horario_envio': stats.get('melhor_horario_envio')
                }
            
            # === EXTRAIR DADOS QUALITATIVOS (APENAS MEMORIA_CONTEXTO) ===
            if not erro_quali and quali_result.get('sucesso'):
                analise = quali_result.get('analise', {})
                dados = analise.get('dados', {})
                fusao['dados_qualitativos'] = {
                    'resumo_interacoes': dados.get('resumo_interacoes_ate_o_momento'),
                    'tom_cliente': dados.get('tom_predominante_cliente'),
                    'status_cliente': dados.get('status_cliente_whatsapp'), 
                    'conversa_aberta': dados.get('conversa_em_aberto'),
                    'pendencia_com': dados.get('pendencia_com_quem')
                }
            
            return fusao
        
        # Executar fusão
        analise_fusionada = fusionar_analises(resultado_quanti, resultado_quali)
        
        logger.info(f"✅ Memória contexto obtida para {request.numero_telefone}")
        
        # === RETORNO ESTRUTURADO (SÓ O ESSENCIAL) ===
        return {
            'sucesso': True,
            'numero_telefone': request.numero_telefone,
            'timestamp_analise': datetime.now().isoformat(),
            'parametros': {
                'teto_minutos': request.teto_minutos or 30,
                'temperatura': request.temperatura or 0.1,
                'modelo': request.modelo
            },
            'memoria_contexto': analise_fusionada,
            'erro': None
        }
    
    except Exception as e:
        error_msg = f"Erro ao obter memória contexto: {str(e)}"
        logger.error(f"❌ {error_msg}")
        
        return {
            'sucesso': False,
            'numero_telefone': request.numero_telefone,
            'erro': error_msg,
            'memoria_contexto': None
        }

# Inclua o roteador sem middlewares adicionais
app.include_router(
    chat_router, 
    prefix="/chat", 
    tags=["chat"]
)

app.include_router(
    debug_router,
    prefix="/debug",
    tags=["debug"]
)

app.include_router(
    activation_router,
    prefix="/activation", 
    tags=["activation"]
)

app.include_router(
    context_router,
    prefix="/context", 
    tags=["context"]
)



# Rota para verificar a saúde da aplicação
@app.get("/health", summary="Verificar saúde da aplicação")
async def health_check():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

