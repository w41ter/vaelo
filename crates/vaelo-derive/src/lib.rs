// SPDX-License-Identifier: Apache-2.0

use proc_macro::{Delimiter, TokenStream, TokenTree};

#[proc_macro_derive(StaticResource)]
pub fn derive_static_resource(input: TokenStream) -> TokenStream {
    match resource_type_name(input) {
        Ok(name) => format!(
            "impl ::vaelo::StaticResource for {name} {{
                fn resource_pool() -> &'static ::vaelo::ResourcePool<Self> {{
                    static POOL: ::std::sync::LazyLock<::vaelo::ResourcePool<{name}>> =
                        ::std::sync::LazyLock::new(|| {{
                            ::vaelo::ResourcePool::new(::vaelo::ResourcePoolConfig::default())
                        }});
                    &POOL
                }}

                fn try_with_resource_local<R>(
                    operation: impl FnOnce(&mut ::vaelo::ResourceLocal<Self>) -> R,
                ) -> Option<R> {{
                    ::std::thread_local! {{
                        static LOCAL: ::std::cell::RefCell<::vaelo::ResourceLocal<{name}>> =
                            ::std::cell::RefCell::new(::vaelo::ResourceLocal::new(
                                <{name} as ::vaelo::StaticResource>::resource_pool()
                            ));
                    }}
                    LOCAL
                        .try_with(|local| operation(&mut local.borrow_mut()))
                        .ok()
                }}
            }}"
        )
        .parse()
        .expect("generated Resource impl should parse"),
        Err(message) => compile_error(&message),
    }
}

fn resource_type_name(input: TokenStream) -> Result<String, String> {
    let mut saw_kind = false;
    let mut name = None;
    for token in input {
        match token {
            TokenTree::Ident(ident)
                if ident.to_string() == "struct" || ident.to_string() == "enum" =>
            {
                saw_kind = true;
            }
            TokenTree::Ident(ident) if saw_kind && name.is_none() => {
                name = Some(ident.to_string());
            }
            TokenTree::Punct(punct) if name.is_some() && punct.as_char() == '<' => {
                return Err(
                    "#[derive(StaticResource)] does not support generic resource types".into()
                );
            }
            TokenTree::Group(group) if name.is_some() && group.delimiter() == Delimiter::Brace => {
                break;
            }
            _ => {}
        }
    }
    name.ok_or_else(|| "#[derive(StaticResource)] only supports structs and enums".into())
}

fn compile_error(message: &str) -> TokenStream {
    format!("compile_error!({message:?});").parse().expect("compile_error should parse")
}
