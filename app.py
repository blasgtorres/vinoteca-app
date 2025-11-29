import streamlit as st
import pandas as pd
from streamlit_gsheets import GSheetsConnection
from duckduckgo_search import DDGS
import requests
from io import BytesIO
import base64
import time
import random
from datetime import datetime
import traceback

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(
    page_title="Mi Vinoteca V5.5",
    page_icon="🍷",
    layout="wide"
)

# --- CONSTANTES ---
UBICACIONES = [
    'Por Clasificar',
    'Cava Eléctrica',
    'Mueble Norte - Botelleros',
    'Mueble Norte - Bandejas',
    'Mueble Este - Bandejas',
    'Mueble Este - Cajonera',
    'Mueble Este - X Grande',
    'Mueble Este - Media X',
    'Mueble Sur - Retícula Superior',
    'Mueble Sur - X Izquierda',
    'Mueble Sur - X Centro',
    'Mueble Sur - X Derecha',
    'Otro',
    'Consumido' # Ubicación especial para historial
]

UVAS_BASE_ESTANDAR = [
    'Malbec', 'Cabernet Sauvignon', 'Merlot', 'Syrah', 'Chardonnay', 
    'Pinot Noir', 'Torrontés', 'Bonarda', 'Petit Verdot', 'Cabernet Franc', 
    'Blend', 'Otro'
]

# --- BASE DE DATOS ---
# --- BASE DE DATOS (GOOGLE SHEETS) ---
def get_conn():
    return st.connection("gsheets", type=GSheetsConnection)

def cargar_vinos():
    conn = get_conn()
    for attempt in range(3):
        try:
            # TTL=600 (10 min) para evitar error 429 Quota Exceeded
            df = conn.read(ttl=600)
            # Convertir a numérico
            cols_num = ['id', 'anada', 'anio_limite', 'puntuacion']
            for c in cols_num:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).astype(int)
            
            # Manejo de imágenes (Hex str -> Bytes)
            if 'imagen_data' in df.columns:
                 df['imagen_data'] = df['imagen_data'].apply(
                     lambda x: bytes.fromhex(x) if isinstance(x, str) and x else None
                 )
                 
            return df
        except Exception as e:
            if attempt < 2:
                # Backoff progresivo: 2s, luego 5s
                wait_time = 2 if attempt == 0 else 5
                time.sleep(wait_time)
                continue
            else:
                st.error(f"Error persistente leyendo Google Sheets: {e}")
                return pd.DataFrame()
    return pd.DataFrame()


def safe_update(df_to_save):
    """
    Función de guardado seguro que valida que el DataFrame no esté vacío
    antes de enviar los datos a Google Sheets.
    """
    if df_to_save is None or df_to_save.empty:
        st.error('⛔ BLOQUEO DE SEGURIDAD: La app intentó borrar todos los datos. Operación cancelada.')
        st.stop()
    
    conn = get_conn()
    conn.update(data=df_to_save)
    st.cache_data.clear()
    st.success("✅ ¡Guardado en la nube correctamente!")
    time.sleep(2)
    st.rerun()



def guardar_vino(datos):
    conn = get_conn()
    df = cargar_vinos()
    
    # Generar ID
    new_id = 1
    if not df.empty and 'id' in df.columns:
        new_id = df['id'].max() + 1
        
    # Desempaquetar datos
    (nombre, bodega, enologo, anada, uva_principal, composicion_blend, 
     gama, procedencia, detalle, nota_cata, ubicacion, anio_limite, 
     puntuacion, imagen_data, tipo_imagen) = datos
     
    # Convertir imagen a hex para storage
    img_hex = imagen_data.hex() if imagen_data else None
    
    new_row = pd.DataFrame([{
        'id': new_id,
        'nombre': nombre, 'bodega': bodega, 'enologo': enologo, 'anada': anada,
        'uva_principal': uva_principal, 'composicion_blend': composicion_blend,
        'gama': gama, 'procedencia': procedencia, 'detalle': detalle,
        'nota_cata': nota_cata, 'ubicacion': ubicacion, 'anio_limite': anio_limite,
        'puntuacion': puntuacion, 'imagen_data': img_hex, 'tipo_imagen': tipo_imagen
    }])
    
    df_updated = pd.concat([df, new_row], ignore_index=True)
    safe_update(df_updated)


def actualizar_vino(id_vino, datos):
    conn = get_conn()
    df = cargar_vinos()
    
    if not df.empty and 'id' in df.columns:
        idx = df[df['id'] == id_vino].index
        if not idx.empty:
            i = idx[0]
            # Desempaquetar
            (nombre, bodega, enologo, anada, uva_principal, composicion_blend, 
             gama, procedencia, detalle, nota_cata, ubicacion, anio_limite, 
             puntuacion, imagen_data, tipo_imagen) = datos
             
            img_hex = imagen_data.hex() if imagen_data else None
            
            df.at[i, 'nombre'] = nombre
            df.at[i, 'bodega'] = bodega
            df.at[i, 'enologo'] = enologo
            df.at[i, 'anada'] = anada
            df.at[i, 'uva_principal'] = uva_principal
            df.at[i, 'composicion_blend'] = composicion_blend
            df.at[i, 'gama'] = gama
            df.at[i, 'procedencia'] = procedencia
            df.at[i, 'detalle'] = detalle
            df.at[i, 'nota_cata'] = nota_cata
            df.at[i, 'ubicacion'] = ubicacion
            df.at[i, 'anio_limite'] = anio_limite
            df.at[i, 'puntuacion'] = puntuacion
            df.at[i, 'imagen_data'] = img_hex
            df.at[i, 'tipo_imagen'] = tipo_imagen
            
            
            safe_update(df)


def registrar_consumo(id_vino):
    conn = get_conn()
    df = cargar_vinos()
    if not df.empty and 'id' in df.columns:
        idx = df[df['id'] == id_vino].index
        if not idx.empty:
            df.at[idx[0], 'ubicacion'] = 'Consumido'
            safe_update(df)


def restaurar_vino(id_vino):
    conn = get_conn()
    df = cargar_vinos()
    if not df.empty and 'id' in df.columns:
        idx = df[df['id'] == id_vino].index
        if not idx.empty:
            df.at[idx[0], 'ubicacion'] = 'Por Clasificar'
            safe_update(df)


def borrar_vino(id_vino):
    conn = get_conn()
    df = cargar_vinos()
    if not df.empty and 'id' in df.columns:
        df = df[df['id'] != id_vino]
        safe_update(df)


def borrar_por_clasificar():
    conn = get_conn()
    df = cargar_vinos()
    if not df.empty and 'ubicacion' in df.columns:
        len_before = len(df)
        df = df[df['ubicacion'] != 'Por Clasificar']
        len_after = len(df)
        safe_update(df)

        return len_before - len_after
    return 0

def obtener_vino_por_id(id_vino):
    df = cargar_vinos()
    if not df.empty and 'id' in df.columns:
        row = df[df['id'] == id_vino]
        if not row.empty:
            return row.iloc[0].to_dict()
    return None

def obtener_uvas_unicas_bd():
    df = cargar_vinos()
    if not df.empty and 'uva_principal' in df.columns:
        return [u for u in df['uva_principal'].dropna().unique() if u and str(u).strip()]
    return []

# --- UTILIDADES ---
def buscar_imagen_ddg(query):
    try:
        with DDGS() as ddgs:
            results = list(ddgs.images(query, max_results=3))
            if results:
                return results[0]['image']
    except Exception as e:
        st.error(f"Error buscando imagen: {e}")
    return None

def buscar_enologo_ddg(query):
    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=1))
            if results:
                return results[0]['body']
    except Exception as e:
        return f"Error buscando enólogo: {e}"
    return "No encontrado"

def descargar_imagen(url):
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            return response.content, response.headers['Content-Type']
    except Exception as e:
        st.error(f"Error descargando imagen: {e}")
    return None, None

def blob_to_b64(blob, mime_type):
    if blob:
        b64 = base64.b64encode(blob).decode('utf-8')
        return f"data:{mime_type};base64,{b64}"
    return None

def normalize_column_name(col_name):
    return str(col_name).strip().lower()

def get_column_value(row, possible_names, default=''):
    for name in possible_names:
        if name in row.index:
            val = row[name]
            if pd.notna(val):
                return str(val).strip()
    return default

# --- INICIALIZACIÓN ---
# init_db() # Removed

if 'selected_id' not in st.session_state:
    st.session_state.selected_id = None

# --- INTERFAZ PRINCIPAL ---
st.title("🍷 Mi Vinoteca V5.5")

# Cargar datos globales
df_todos = cargar_vinos()
if not df_todos.empty:
    df_todos['imagen_visual'] = df_todos.apply(
        lambda row: blob_to_b64(row['imagen_data'], row['tipo_imagen']), axis=1
    )

# Configuración de Columnas Común
column_config = {
    "id": st.column_config.NumberColumn("ID", format="%d", width="small"),
    "imagen_visual": st.column_config.ImageColumn("Foto", width="small"),
    "puntuacion": st.column_config.ProgressColumn("Pts", min_value=0, max_value=10, format="%d"),
    "anio_limite": st.column_config.NumberColumn("Hasta", format="%d"),
    "anada": st.column_config.NumberColumn("Añada", format="%d"),
    "uva_principal": "Uva",
    "composicion_blend": "Corte",
    "gama": "Gama",
    "procedencia": "Procedencia",
    "enologo": "Enólogo",
    "ubicacion": "Ubicación",
    "detalle": "Detalle Técnico",
    "nota_cata": "Nota Personal"
}

cols_order = [
    'id', 'nombre', 'bodega', 'uva_principal', 'composicion_blend', 'anada', 'anio_limite', 
    'gama', 'procedencia', 'enologo', 'ubicacion', 'detalle', 'nota_cata', 'puntuacion'
]

# --- TABS ---
tab_bodega, tab_sommelier, tab_historial = st.tabs(['🍾 Bodega Activa', '🤖 Sommelier Virtual', '📜 Historial Bebido'])

# ==========================================
# TAB 1: BODEGA ACTIVA
# ==========================================
with tab_bodega:
    st.subheader("Inventario Actual")
    
    if not df_todos.empty:
        df_activos = df_todos[df_todos['ubicacion'] != 'Consumido']
        
        cols_final = [c for c in cols_order if c in df_activos.columns]
        
        event = st.dataframe(
            df_activos[cols_final],
            column_config=column_config,
            height=500,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
            key="vinos_table_activos"
        )
        
        if event.selection.rows:
            idx = event.selection.rows[0]
            row_data = df_activos.iloc[idx]
            st.session_state.selected_id = int(row_data['id'])
        else:
            # Si no hay selección en ESTA tabla, no hacemos nada para no romper el flujo
            pass 
            
    else:
        st.info("No hay vinos cargados.")

# ==========================================
# TAB 2: SOMMELIER VIRTUAL
# ==========================================
with tab_sommelier:
    st.subheader("🤖 Tu Sommelier Personal")
    
    if not df_todos.empty:
        df_activos = df_todos[df_todos['ubicacion'] != 'Consumido']
        
        # Filtros Avanzados
        col_f1, col_f2, col_f3 = st.columns(3)
        
        with col_f1:
            solo_vencer = st.checkbox("🚨 Mostrar solo vinos por vencer")
            
            uvas_disp = sorted(list(set(df_activos['uva_principal'].dropna().unique())))
            filtro_uva = st.multiselect("Uva / Corte", uvas_disp)
            
        with col_f2:
            bodegas_disp = sorted(list(set(df_activos['bodega'].dropna().unique())))
            filtro_bodega = st.multiselect("Bodega", bodegas_disp)
            
            anios_disp = sorted(list(set(df_activos['anada'].dropna().unique())))
            filtro_anada = st.multiselect("Añada", anios_disp)
            
        with col_f3:
            procedencias_disp = sorted(list(set(df_activos['procedencia'].dropna().unique())))
            filtro_proc = st.multiselect("Procedencia", procedencias_disp)
            
            gamas_disp = sorted(list(set(df_activos['gama'].dropna().unique())))
            filtro_gama = st.multiselect("Gama", gamas_disp)
            
            enos_disp = sorted(list(set(df_activos['enologo'].dropna().unique())))
            filtro_eno = st.multiselect("Enólogo", enos_disp)
            
        st.markdown("---")
        
        if st.button("🎲 RECOMENDARME UN VINO", type="primary", use_container_width=True):
            # Aplicar Filtros (AND Logic)
            df_filtrado = df_activos.copy()
            
            if solo_vencer:
                anio_actual = datetime.now().year
                df_filtrado = df_filtrado[df_filtrado['anio_limite'] <= anio_actual]
                
            if filtro_uva:
                df_filtrado = df_filtrado[df_filtrado['uva_principal'].isin(filtro_uva)]
            
            if filtro_bodega:
                df_filtrado = df_filtrado[df_filtrado['bodega'].isin(filtro_bodega)]
                
            if filtro_anada:
                df_filtrado = df_filtrado[df_filtrado['anada'].isin(filtro_anada)]
                
            if filtro_proc:
                df_filtrado = df_filtrado[df_filtrado['procedencia'].isin(filtro_proc)]
                
            if filtro_gama:
                df_filtrado = df_filtrado[df_filtrado['gama'].isin(filtro_gama)]
                
            if filtro_eno:
                df_filtrado = df_filtrado[df_filtrado['enologo'].isin(filtro_eno)]
                
            if not df_filtrado.empty:
                # Elegir Random
                elegido = df_filtrado.sample(1).iloc[0]
                
                st.success("¡El Sommelier ha elegido!")
                st.markdown("---")
                
                c1, c2 = st.columns([1, 2])
                with c1:
                    if elegido['imagen_visual']:
                        st.image(elegido['imagen_visual'], width=200)
                    else:
                        st.write("🍷 Sin foto")
                with c2:
                    st.markdown(f"# {elegido['nombre']}")
                    st.markdown(f"### {elegido['bodega']}")
                    st.markdown(f"**Añada:** {int(elegido['anada']) if pd.notna(elegido['anada']) else '-'}")
                    st.markdown(f"**Uva:** {elegido['uva_principal']}")
                    if elegido['composicion_blend']:
                        st.markdown(f"**Corte:** {elegido['composicion_blend']}")
                    st.markdown(f"**Ubicación:** {elegido['ubicacion']}")
                    st.markdown(f"**Procedencia:** {elegido['procedencia']}")
                    
            else:
                st.warning("No hay vinos que coincidan con TODOS tus criterios. ¡Prueba relajar los filtros!")

# ==========================================
# TAB 3: HISTORIAL BEBIDO
# ==========================================
with tab_historial:
    st.subheader("📜 Historial de Consumo")
    
    if not df_todos.empty:
        df_consumidos = df_todos[df_todos['ubicacion'] == 'Consumido']
        
        if not df_consumidos.empty:
            cols_final = [c for c in cols_order if c in df_consumidos.columns]
            
            event_h = st.dataframe(
                df_consumidos[cols_final],
                column_config=column_config,
                height=500,
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row",
                key="vinos_table_historial"
            )
            
            if event_h.selection.rows:
                idx = event_h.selection.rows[0]
                row_data = df_consumidos.iloc[idx]
                st.session_state.selected_id = int(row_data['id'])
        else:
            st.info("Aún no has bebido ningún vino. ¡Salud!")


# --- SIDEBAR (COMÚN) ---
with st.sidebar:
    st.header("Gestión de Vinos")
    
    modo_edicion = st.session_state.selected_id is not None
    
    if modo_edicion:
        st.success(f"✏️ Editando ID: {st.session_state.selected_id}")
        vino_data = obtener_vino_por_id(st.session_state.selected_id)
        if not vino_data:
            st.error("Error cargando vino.")
            # st.stop() # No stop para permitir cargar nuevo
        else:
            # BOTÓN DE CONSUMO (Solo si NO está consumido)
            if vino_data['ubicacion'] != 'Consumido':
                st.markdown("---")
                if st.button("🍷 ¡REGISTRAR COMO BEBIDO!", type="primary"):
                    st.balloons()
                    # Guardamos ID antes de limpiar
                    id_temp = st.session_state.selected_id
                    st.session_state.selected_id = None
                    registrar_consumo(id_temp)

                st.markdown("---")
            
            # BOTÓN DE RESTAURAR (Solo si ESTÁ consumido)
            elif vino_data['ubicacion'] == 'Consumido':
                st.markdown("---")
                if st.button("♻️ RESTAURAR A BODEGA"): # Botón normal (amarillo/gris según tema, pero distinto al primary)
                    id_temp = st.session_state.selected_id
                    st.session_state.selected_id = None
                    restaurar_vino(id_temp)

                st.markdown("---")
            
            if st.button("➕ Cancelar / Nuevo"):
                st.session_state.selected_id = None
                st.rerun()
    else:
        st.info("➕ Modo Carga Nuevo")

    # --- FORMULARIO ---
    if modo_edicion and vino_data:
        def_nombre = vino_data['nombre']
        def_bodega = vino_data['bodega']
        def_enologo = vino_data['enologo']
        def_anada = int(vino_data['anada']) if vino_data['anada'] else 2023
        def_uva_princ = vino_data['uva_principal']
        def_comp_blend = vino_data['composicion_blend']
        def_gama = vino_data['gama']
        def_procedencia = vino_data['procedencia']
        def_detalle = vino_data['detalle']
        def_nota_cata = vino_data['nota_cata']
        def_ubicacion = vino_data['ubicacion']
        def_anio_limite = int(vino_data['anio_limite']) if vino_data['anio_limite'] else 2030
        def_puntuacion = int(vino_data['puntuacion']) if vino_data['puntuacion'] else 5
        
        if 'imagen_confirmada_blob' not in st.session_state:
             st.session_state.imagen_confirmada_blob = vino_data['imagen_data']
             st.session_state.imagen_confirmada_mime = vino_data['tipo_imagen']
    else:
        def_nombre = ""
        def_bodega = ""
        def_enologo = ""
        def_anada = 2023
        def_uva_princ = "Malbec"
        def_comp_blend = ""
        def_gama = ""
        def_procedencia = ""
        def_detalle = ""
        def_nota_cata = ""
        def_ubicacion = "Cava Eléctrica"
        def_anio_limite = 2030
        def_puntuacion = 5
        
    nombre = st.text_input("Nombre", value=def_nombre)
    bodega = st.text_input("Bodega", value=def_bodega)
    gama = st.text_input("Gama / Calidad", value=def_gama)
    
    col_eno_in, col_eno_btn = st.columns([3, 1])
    with col_eno_in:
        enologo = st.text_input("Enólogo", value=def_enologo)
    with col_eno_btn:
        if st.button("🔍"):
            if enologo or (nombre and bodega):
                q = f"{enologo} {nombre} {bodega} winemaker"
                info = buscar_enologo_ddg(q)
                st.info(info[:150])

    anada = st.number_input("Añada", 1900, 2100, def_anada)
    
    # --- LÓGICA UNIFICADA DE LISTAS DE UVAS ---
    uvas_bd = obtener_uvas_unicas_bd()
    lista_maestra = sorted(list(set(uvas_bd + UVAS_BASE_ESTANDAR)))
    
    if def_uva_princ and def_uva_princ not in lista_maestra:
        lista_maestra.append(def_uva_princ)
        lista_maestra = sorted(lista_maestra)
        
    try:
        idx_uva = lista_maestra.index(def_uva_princ)
    except ValueError:
        idx_uva = lista_maestra.index('Otro') if 'Otro' in lista_maestra else 0
        
    uva_sel = st.selectbox("Variedad", lista_maestra, index=idx_uva)
    
    uva_final = uva_sel
    comp_final = ""
    
    if uva_sel == 'Blend':
        default_multiselect = []
        opciones_blend = [u for u in lista_maestra if u not in ['Blend', 'Otro']]
        
        if modo_edicion and def_comp_blend:
             parts = [x.strip() for x in def_comp_blend.split(',')]
             default_multiselect = [p for p in parts if p in opciones_blend]
        
        componentes = st.multiselect("Composición", opciones_blend, default=default_multiselect)
        if componentes:
            comp_final = ", ".join(componentes)
    elif uva_sel == 'Otro':
        otra = st.text_input("Especifique", value="")
        if otra:
            uva_final = otra
            
    ubicacion = st.selectbox("Ubicación", UBICACIONES, index=UBICACIONES.index(def_ubicacion) if def_ubicacion in UBICACIONES else 0)
    procedencia = st.text_input("Procedencia", value=def_procedencia)
    
    detalle = st.text_area("Detalle (Técnico)", value=def_detalle, height=100)
    nota_cata = st.text_area("Nota de Cata (Personal)", value=def_nota_cata, height=100)
    
    anio_limite = st.number_input("Consumo Ideal (Hasta)", 2024, 2050, def_anio_limite)
    puntuacion = st.slider("Puntos", 1, 10, def_puntuacion)
    
    # Imagen
    st.markdown("### Imagen")
    if st.button("🔮 Buscar Auto"):
        with st.spinner("..."):
            url = buscar_imagen_ddg(f"{nombre} {bodega} bottle")
            if url:
                st.session_state.imagen_candidata_url = url
    
    if st.session_state.get('imagen_candidata_url'):
        st.image(st.session_state.imagen_candidata_url, width=100)
        if st.button("✅ Usar"):
            b, m = descargar_imagen(st.session_state.imagen_candidata_url)
            if b:
                st.session_state.imagen_confirmada_blob = b
                st.session_state.imagen_confirmada_mime = m
                st.session_state.imagen_candidata_url = None
                
    uploaded = st.file_uploader("Subir", type=['jpg','png'])
    if uploaded:
        st.session_state.imagen_confirmada_blob = uploaded.getvalue()
        st.session_state.imagen_confirmada_mime = uploaded.type
        
    st.markdown("---")
    lbl_save = "💾 Actualizar" if modo_edicion else "💾 Guardar Nuevo"
    
    if st.button(lbl_save, type="primary"):
        if nombre and bodega:
            blob = st.session_state.get('imagen_confirmada_blob')
            mime = st.session_state.get('imagen_confirmada_mime')
            
            datos = (
                nombre, bodega, enologo, anada, uva_final, comp_final,
                gama, procedencia, detalle, nota_cata, ubicacion, anio_limite, puntuacion,
                blob, mime
            )
            
            if modo_edicion:
                actualizar_vino(st.session_state.selected_id, datos)
            else:
                guardar_vino(datos)

        else:
            st.error("Falta Nombre/Bodega")
            
    # Botón Eliminar
    if modo_edicion:
        st.markdown("---")
        if st.button("🗑️ ELIMINAR VINO", type="primary"): 
            id_temp = st.session_state.selected_id
            st.session_state.selected_id = None
            borrar_vino(id_temp)


    # --- IMPORTACIÓN / LIMPIEZA ---
    with st.expander("⚙️ Importar / Limpiar"):
        up_file = st.file_uploader("Excel/CSV", type=['xlsx','csv'])
        if up_file and st.button("Importar"):
            try:
                # 1. Leer archivo
                if up_file.name.endswith('.csv'):
                    df_nuevo = pd.read_csv(up_file)
                else:
                    df_nuevo = pd.read_excel(up_file)
                
                # Normalizar columnas
                df_nuevo.columns = [normalize_column_name(c) for c in df_nuevo.columns]
                
                # 2. Leer datos actuales
                df_actual = cargar_vinos()
                
                # 3. Calcular ID Inicial
                ultimo_id = 0
                if not df_actual.empty and 'id' in df_actual.columns:
                    # Forzar conversión a int de IDs existentes
                    try:
                        ids_existentes = pd.to_numeric(df_actual['id'], errors='coerce').fillna(0).astype(int)
                        if not ids_existentes.empty:
                            ultimo_id = ids_existentes.max()
                    except:
                        ultimo_id = 0
                
                nuevos_vinos = []
                bar = st.progress(0)
                tot = len(df_nuevo)
                
                # 4. Iteración con Sanitización TOTAL
                for i, row in df_nuevo.iterrows():
                    # Helpers de limpieza
                    def clean_str(val):
                        if pd.isna(val): return ""
                        return str(val).strip()
                    
                    def clean_int(val, default):
                        try:
                            if pd.isna(val): return default
                            return int(float(str(val)))
                        except:
                            return default
                            
                    # Extracción y Limpieza
                    nom = clean_str(get_column_value(row, ['nombre','name'], 'Sin Nombre'))
                    bod = clean_str(get_column_value(row, ['bodega','winery'], 'Desconocida'))
                    eno = clean_str(get_column_value(row, ['enologo'], ''))
                    
                    ana = clean_int(get_column_value(row, ['anada','vintage','year'], 2023), 2023)
                    
                    # Uva
                    var_raw = clean_str(get_column_value(row, ['variedad','uva','tipo','grape'], 'Otro'))
                    if '/' in var_raw: 
                        u_princ = 'Blend'
                        c_blend = var_raw
                    else:
                        u_princ = var_raw
                        c_blend = ''
                        
                    gam = clean_str(get_column_value(row, ['gama','calidad'], ''))
                    pro = clean_str(get_column_value(row, ['procedencia','region'], ''))
                    det = clean_str(get_column_value(row, ['detalle','notas','notes','description'], ''))
                    
                    anio_lim = clean_int(get_column_value(row, ['anio_limite','consumo'], 2030), 2030)
                    pts = clean_int(get_column_value(row, ['puntuacion','puntos'], 5), 5)
                    
                    ultimo_id += 1
                    
                    # Construcción de Diccionario Seguro
                    vino = {
                        'id': int(ultimo_id),
                        'nombre': str(nom),
                        'bodega': str(bod),
                        'enologo': str(eno),
                        'anada': int(ana),
                        'uva_principal': str(u_princ),
                        'composicion_blend': str(c_blend),
                        'gama': str(gam),
                        'procedencia': str(pro),
                        'detalle': str(det),
                        'nota_cata': "",
                        'ubicacion': "Por Clasificar",
                        'anio_limite': int(anio_lim),
                        'puntuacion': int(pts),
                        'imagen_data': None,
                        'tipo_imagen': None
                    }
                    nuevos_vinos.append(vino)
                    bar.progress((i+1)/tot)
                
                # 5. Crear DataFrame Final
                if nuevos_vinos:
                    df_to_add = pd.DataFrame(nuevos_vinos)
                    
                    # Concatenar
                    df_final = pd.concat([df_actual, df_to_add], ignore_index=True)
                    
                    # Reemplazo final de NaN por si acaso
                    df_final = df_final.fillna("")
                    
                    # Guardar
                    safe_update(df_final)

                else:
                    st.warning("No se encontraron datos para importar.")
                    
            except Exception as e:
                st.error(f"Ocurrió un error: {e}")
                st.code(traceback.format_exc())
                
        if st.checkbox("Borrado Masivo"):
            if st.button("🗑️ Borrar 'Por Clasificar'"):
                n = borrar_por_clasificar()

